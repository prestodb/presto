package com.facebook.presto.spark;

import static java.util.Objects.requireNonNull;
import static scala.collection.JavaConversions.asScalaBuffer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskProcessor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskSourceRdd;
import com.facebook.spark.protocol.TaskResult;
import com.facebook.spark.protocol.TaskSubmit;
import fbshade0.org.apache.thrift.protocol.TBinaryProtocol;
import fbshade0.org.apache.thrift.protocol.TBinaryProtocol.Factory;
import fbshade0.org.apache.thrift.protocol.TProtocol;
import fbshade0.org.apache.thrift.transport.TIOStreamTransport;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ZippedPartitionsBaseRDD;
import org.apache.spark.util.CircularBuffer;
import org.apache.spark.util.RedirectThread;
import org.apache.spark.util.ShutdownHookManager;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

public class NativeEngineRdd <T extends PrestoSparkTaskOutput>
        extends ZippedPartitionsBaseRDD<Tuple2<MutablePartitionId, T>> {

  private static class NativeEngineClient{

    final int libraryVersion = 1;
    final byte[] header = "NATIVE".getBytes();
    final String initTimeout = "20s";
    // data bytes buffer (4-bytes Integer)
    private final ByteBuffer bytesBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
    private InputStream input;
    private OutputStream output;

    public NativeEngineClient(InputStream input,
        OutputStream output) throws IOException {
      this.input = input;
      this.output = output;
    }

    /**
     * Binary protocol initialization for Native Execution.
     */
    private void init() throws IOException {
      // logInfo(s"NativeEngine protocol init. Library version: $libraryVersion")

      // logInfo(s"Reading NativeEngine init frame from native engine process...")
      byte[] receivedHeaderBytes = new byte[header.length];

      input.read(receivedHeaderBytes);

      if (Arrays.equals(header, receivedHeaderBytes)) {
        throw new RuntimeException("NativeEngine init was not received from native engine process");
      }

      byte[] clientLibraryVersionBytes = new byte[Integer.BYTES];
      input.read(clientLibraryVersionBytes);
      int clientLibraryVersion = ByteBuffer.wrap(clientLibraryVersionBytes)
          .order(ByteOrder.nativeOrder()).getInt();

      if (libraryVersion != clientLibraryVersion) {
        throw new RuntimeException(
            "Library version mismatch. Spark library version: " + libraryVersion +
                ", Native Engine process library version: " + clientLibraryVersion);
      }

      // send NativeEngine init acknowledgment to native engine process
      output.write(header);
      writeFrameLength(libraryVersion);
    }

    /**
     * Send Spark plan and meta data to native evaluation engine
     */
    private void sendSparkPlan(TaskSubmit taskSubmit) throws IOException {
      if (taskSubmit == null) {
        throw new RuntimeException("TaskSubmit is not defined!");

      }
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      TProtocol binaryProtocol = new TBinaryProtocol.Factory().getProtocol(new TIOStreamTransport(byteArrayOutputStream));

      // Serialize the spark plan (in TaskSubmit object) to thrift binary plan representation
      taskSubmit.write0(binaryProtocol);
      // get the bytes array of the serialized thrift binary plan object
      byte[] binaryBytes = byteArrayOutputStream.toByteArray();
      // send the length of the total bytes (of the thrift object) to external process
      writeFrameLength(binaryBytes.length);
      // send the actual bytes array to external process
      output.write(binaryBytes);
      output.flush();
      bytesBuffer.clear();
      log.info("Sent task information to Native Engine Process");
    }

    private void writeFrameLength(int frameLength) throws IOException {
      output.write(bytesBuffer.putInt(frameLength).array());
      output.flush();
      bytesBuffer.clear();
    }
  }

  /**
   * It is necessary to have a monitor thread for python workers if the user cancels with
   * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise the
   * threads can block indefinitely.
   */
  private static class MonitorThread extends Thread {

    private ProcessBuilder scriptProcess;
    private CircularBuffer stderrBuffer;
    private String tmpDir;
    private File logFile;
    private Optional<TaskContext> context;

    public MonitorThread(
        Optional<TaskContext> context,
        ProcessBuilder scriptProcess,
        CircularBuffer stderrBuffer) throws InterruptedException {
      this.scriptProcess = scriptProcess;
      this.stderrBuffer = stderrBuffer;
      this.tmpDir = isTesting()? "/tmp" : System.getProperty("java.io.tmpdir");
      this.logFile = new File(System.getenv().getOrDefault(
          "BUMBLEBEE_CONTAINER_LOG_DIR", tmpDir), "transform_stderr");
      this.context = context;
      setDaemon(true);
      setName("Thread-Worker-Monitor");

      String cmd = String.join(" ", scriptProcess.command());
      String headerString = "Script:" + cmd + "\n";
      headerString += "*** Transform stderr log ***";
      logStderrToFile(headerString, false);
    }

    private boolean isTesting() {
      return  (System.getenv("SPARK_TESTING") != null)
          || (System.getenv("spark.testing") != null);
    }
    /**
     * A utility method to log the child process's stderr to file.
     */
    private synchronized void logStderrToFile(String lines) {
      logStderrToFile(lines, true);
    }

    private synchronized void logStderrToFile(String lines, boolean resetStderrBuffer) {
      if (!lines.isEmpty()) {
        try {
          BufferedWriter stderrWriter = new BufferedWriter(new FileWriter(logFile, true));
          String prefix = context.map(taskContext -> "(TID " + taskContext.taskAttemptId() + " )")
              .orElse("");
          String[] logLines = lines.split("\n");
          for (int i = 0; i < logLines.length; ++i) {
            stderrWriter.write(prefix + logLines[i] + "\n");
          }
//          if (resetStderrBuffer) {
//            stderrBuffer.reset();
//          }
        } catch  (IOException e) {
          throw new RuntimeException("Failed to write '${logFile.getAbsolutePath", e);
        }
      }
    }

    private void logStderrAndSleep() throws InterruptedException {
      logStderrToFile(stderrBuffer.toString());
      Thread.sleep(5000);    // flush logs every 5 seconds in local-mode
    }

    @Override
    public void run() {
      if (context.isPresent()) {
        // Only run the thread if the task if is not completed nor interrupted.
        while (!context.get().isInterrupted() && !context.get().isCompleted()) {
          try {
            logStderrAndSleep();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        while (true) {
          try {
            logStderrAndSleep();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public void interrupt() {
      logStderrToFile(stderrBuffer.toString());
    }
  }

  private static class ResidentNativeEngineReaderThread extends Thread {

    private volatile Throwable _exception = null;
    private final InputStream inputStream;
    private final ConcurrentHashMap<Long, com.facebook.spark.protocol.TaskResult> tasksResults;

    public ResidentNativeEngineReaderThread(
        InputStream inputStream,
        ConcurrentHashMap<Long, com.facebook.spark.protocol.TaskResult> taskResults) {
      this.inputStream = inputStream;
      this.tasksResults = taskResults;
      setName("Thread-ResidentNativeEngine-Reader");
      setDaemon(true);
    }

    /**
     * Contains the exception thrown while reading from the external process.
     */
    public Optional<Throwable> exception() {
     if(_exception != null) {
       return Optional.of(_exception);
     } else {
       return Optional.empty();
     }
    }

    public static byte[] toByteArray(InputStream input, int size) throws IOException {
      if (size < 0) {
        throw new IllegalArgumentException("Size must be equal or greater than zero: " + size);
      } else if (size == 0) {
        return new byte[0];
      } else {
        byte[] data = new byte[size];

        int offset;
        int readed;
        for (offset = 0; offset < size && (readed = input.read(data, offset, size - offset)) != -1; offset += readed) {
        }

        if (offset != size) {
          throw new IOException("Unexpected readed size. current: " + offset + ", excepted: " + size);
        } else {
          return data;
        }
      }
    }

    private byte[] readBytes(int toRead, String what) throws IOException {
        return toByteArray(inputStream, toRead);
    }

  /*
  Deserialize the byte streams of the TaskResult thrift object from external process.
  */
    public TaskResult deserializeTaskResult(byte[] bytes) {
        Factory protocolFactory = new TBinaryProtocol.Factory();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        TIOStreamTransport transport = new TIOStreamTransport(byteArrayInputStream);
        return TaskResult.read0(protocolFactory.getProtocol(transport));
    }

    /**
     * Read TaskResult thrift object from transform process.
     * and add it to TasksResults map
     */
    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          // get data length for task result
          int  dataLength =
              ByteBuffer.wrap(readBytes(Integer.BYTES, "task result data length"))
                  .order(ByteOrder.nativeOrder()).getInt();

          // get data bytes for task metric
          byte[] taskResultBytes = readBytes(dataLength, "task result");
          TaskResult taskResult = deserializeTaskResult(taskResultBytes);
          tasksResults.put(taskResult.getTaskId(), taskResult);
        } catch  (Throwable e) {
            // ignore exceptions if occurred as a results of thread being interrupted
            if (!Thread.interrupted()) {
              _exception = e;
              log.error("Exception occurred in reader thread: " + e.getMessage());
              Thread.currentThread().interrupt();
            }
        }
      }
    }
  }

  public static class ResidentNativeEngine {

    private static volatile Optional<Process> process  = Optional.empty();
    private static NativeEngineClient nativeEngineClient = null;
    private static Thread monitorThread = null;
    private static ResidentNativeEngineReaderThread readerThread = null;
    private static final CircularBuffer stderrBuffer = new CircularBuffer(102400); // 100 kilobytes
    // map TaskIDs -> TaskResult
    private static final ConcurrentHashMap<Long, TaskResult> tasksResults = new ConcurrentHashMap<Long, TaskResult>();

    public static TaskResult waitAndGetTaskResult(long taskAttemptId) throws InterruptedException {
      // TODO: use locks on tasksResults keys
      while (tasksResults.get(taskAttemptId) == null) {
        // check for exceptions in the reader thread
        if (!process.get().isAlive() || readerThread.exception().isPresent()) {
          monitorThread.interrupt(); // flush stderr buffer to file
          String errorReason = !process.get().isAlive()?
            "exit code=" + process.get().exitValue() :
            "readerThread exception: " + readerThread.exception().toString();
          throw new RuntimeException(errorReason);
        }
        Thread.sleep(1000);
      }
      monitorThread.interrupt(); // write NativeEngine's stderr to file

     return tasksResults.remove(taskAttemptId);  // remove result from the map and return
    }

    public static void sendSparkPlan(TaskSubmit taskSubmit) throws IOException {
      nativeEngineClient.sendSparkPlan(taskSubmit);
    }

    // stream to read stderr from the NativeEngine process
    private static InputStream getErrorStream() {
      return process.get().getErrorStream();
    }

    private static InputStream getInputStream() {
      return process.get().getInputStream();
    }

    private static OutputStream getOutputStream() {
      return process.get().getOutputStream();
    }

    static void endWord(List<String> buf, StringBuilder curWord) {
      buf.add(curWord.toString());
      curWord.setLength(0);
    }

    private static boolean isSpace(char c) {
      return " \t\r\n".indexOf(c) != -1;
    }

    private static List<String> splitCommandString(String s) {
      List<String> buf = new ArrayList<String>();
      boolean inWord = false;
      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;
      StringBuilder curWord = new StringBuilder();
      int i = 0;
      while (i < s.length()) {
        char nextChar = s.charAt(i);
        if (inDoubleQuote) {
          if (nextChar == '"') {
            inDoubleQuote = false;
          } else if (nextChar == '\\') {
            if (i < s.length() - 1) {
              // Append the next character directly, because only " and \ may be escaped in
              // double quotes after the shell's own expansion
              curWord.append(s.charAt(i + 1));
              i += 1;
            }
          } else {
            curWord.append(nextChar);
          }
        } else if (inSingleQuote) {
          if (nextChar == '\'') {
            inSingleQuote = false;
          } else {
            curWord.append(nextChar);
          }
          // Backslashes are not treated specially in single quotes
        } else if (nextChar == '"') {
          inWord = true;
          inDoubleQuote = true;
        } else if (nextChar == '\'') {
          inWord = true;
          inSingleQuote = true;
        } else if (!isSpace(nextChar)) {
          curWord.append(nextChar);
          inWord = true;
        } else if (inWord && isSpace(nextChar)) {
          endWord(buf, curWord);
          inWord = false;
        }
        i += 1;
      }
      if (inWord || inDoubleQuote || inSingleQuote) {
        endWord(buf, curWord);
      }
      return buf;
    }

    private static ProcessBuilder getProcessBuilder(String script) {
      // Getting script full-path
      File workingDir = new File(SparkFiles.getRootDirectory());
      List<String> args = splitCommandString(script);

      if (!new File(args.get(0)).isAbsolute()) {
        if (new File(workingDir, args.get(0)).exists()) {
          args.set(0, new File(workingDir, args.get(0)).getAbsolutePath());
        }
      }

      if (!new File(args.get(0)).exists()) {
        throw new RuntimeException("File " +  args.get(0) + " does not exist. ");
      }

      // script ProcessBuilder
      ProcessBuilder builder = new ProcessBuilder(args);
      // set working directory
      builder.directory(workingDir);
      // environment variables
      Map<String,String> env = builder.environment();
      env.put("PATH", env.getOrDefault("PATH", "") + workingDir.getAbsolutePath());
      // unset LD_PRELOAD (prevent inheritance from parent)
      env.remove("LD_PRELOAD");
      // Set spark.executor.cores to allow NativeEngine to use it to set threadPoolSize
      // env.put("spark.executor.cores", "4");
      env.put("spark.executor.cores", "1");

      return builder;
    }

    private static void stop() throws IOException, InterruptedException {
      // logInfo("Stopping ResidentNativeEngine...")
      getOutputStream().close();   // close outputStream (process stdin)
      readerThread.interrupt();  // stop reader thread
      int exitCode = process.get().waitFor();
      getInputStream().close();    // close inputStream (process stdout)
      monitorThread.interrupt(); // flush stderr buffer to file
      if (exitCode != 0) {
        throw new RuntimeException("NativeEngine process terminated unexpectedly, exit code=" + process.get().exitValue());
      }
    }

    public static synchronized void startProcess(String script) throws IOException, InterruptedException {
      if (!process.isPresent()) {
        // launch process
        ProcessBuilder processBuilder = getProcessBuilder(script);

        process = Optional.of(processBuilder.start());

        // To avoid issues caused by large error output, we use a circular buffer to limit the
        // amount of error output (stderr) that we retain.
        new RedirectThread(getErrorStream(), stderrBuffer,
            "Thread-ResidentNativeEngine-STDERR-Consumer", false).start();

        // periodically log stderr from buffer to file
        monitorThread = new MonitorThread(null, processBuilder, stderrBuffer);
        monitorThread.start();

        // add shutdown hook
        ShutdownHookManager.addShutdownHook(ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY(), new scala.runtime.AbstractFunction0<BoxedUnit>() {
          @Override
          public BoxedUnit apply() {
            try {
              stop();
            } catch (IOException | InterruptedException e) {
              throw new RuntimeException(e);
            }
            return null;
          }
        });

        // Initialize Spark's NativeEngine client
        nativeEngineClient = new NativeEngineClient(getInputStream(), getOutputStream());
        nativeEngineClient.init();

        // start consuming NativeEngine process output (task metrics that are sent upon completion)
        readerThread = new ResidentNativeEngineReaderThread(getInputStream(), tasksResults);
        readerThread.start();
      }

      if (!process.get().isAlive()) {
        monitorThread.interrupt(); // flush stderr buffer to file
        // Throw fatal exception to shutdown current executor as the external process's dead.
        throw new RuntimeException("NativeEngine process terminated unexpectedly, exit code=" + process.get().exitValue());
      }
    }

  }

  private static class NativeEngineTaskExecutor<T extends PrestoSparkTaskOutput>
      extends AbstractIterator<Tuple2<MutablePartitionId, T>>
      implements IPrestoSparkTaskExecutor<T> {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Tuple2<MutablePartitionId, T> next() {
      return null;
    }
  }

  private static final Logger log = Logger.get(NativeEngineRdd.class);
  private List<String> shuffleInputFragmentIds;
  private List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds;
  private PrestoSparkTaskSourceRdd taskSourceRdd;
  private PrestoSparkTaskProcessor<T> taskProcessor;

  private static <T> ClassTag<T> fakeClassTag()
  {
    return scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
  }

  private static Seq<RDD<?>> getRDDSequence(Optional<PrestoSparkTaskSourceRdd> taskSourceRdd, List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds)
  {
    List<RDD<?>> list = new ArrayList<>(shuffleInputRdds);
    taskSourceRdd.ifPresent(list::add);
    return asScalaBuffer(list).toSeq();
  }

  public static <T extends PrestoSparkTaskOutput> NativeEngineRdd<T> create(
      SparkContext context,
      Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
      // fragmentId -> RDD
      Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap,
      PrestoSparkTaskProcessor<T> taskProcessor)
  {
    requireNonNull(context, "context is null");
    requireNonNull(taskSourceRdd, "taskSourceRdd is null");
    requireNonNull(shuffleInputRddMap, "shuffleInputRdds is null");
    requireNonNull(taskProcessor, "taskProcessor is null");
    List<String> shuffleInputFragmentIds = new ArrayList<>();
    List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds = new ArrayList<>();
    for (Map.Entry<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> entry : shuffleInputRddMap.entrySet()) {
      shuffleInputFragmentIds.add(entry.getKey());
      shuffleInputRdds.add(entry.getValue());
    }
    return new NativeEngineRdd<>(context, taskSourceRdd, shuffleInputFragmentIds, shuffleInputRdds, taskProcessor);
  }

  private NativeEngineRdd(
      SparkContext context,
      Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
      List<String> shuffleInputFragmentIds,
      List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds,
      PrestoSparkTaskProcessor<T> taskProcessor) {
    super(context, getRDDSequence(taskSourceRdd, shuffleInputRdds), false, fakeClassTag());
    this.shuffleInputFragmentIds = shuffleInputFragmentIds;
    this.shuffleInputRdds = shuffleInputRdds;
    // Optional is not Java Serializable
    this.taskSourceRdd = taskSourceRdd.orElse(null);
    this.taskProcessor = context.clean(taskProcessor, true);
  }

  @Override
  public Iterator<Tuple2<MutablePartitionId, T>> compute(Partition split, TaskContext context) {
    log.info("Running in ResidentNativeEngine mode");
    String script = "";
    try {
      TaskSubmit taskSubmit = new TaskSubmit(null, null, null, null, null, null, 0);
      ResidentNativeEngine.startProcess(script + " --resident-mode");
      ResidentNativeEngine.sendSparkPlan(taskSubmit);
      long taskAttemptId = TaskContext.get().taskAttemptId();
      log.info("Waiting for NativeEngine to complete task " + taskAttemptId);
      TaskResult taskResult = ResidentNativeEngine.waitAndGetTaskResult(taskAttemptId);
      // processTaskResult(taskResult)
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
   return new NativeEngineTaskExecutor<T>();
  }

}
