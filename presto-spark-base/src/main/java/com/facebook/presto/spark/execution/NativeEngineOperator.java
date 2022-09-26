/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.SplitOperatorInfo;
import com.facebook.presto.server.localtask.LocalTask;
import com.facebook.presto.server.localtask.LocalTaskFactory;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.NativeEngineSplitWrapper;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.spark.protocol.PrestoTaskSubmit;
import com.facebook.spark.protocol.TaskResult;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import fbshade0.org.apache.thrift.protocol.TBinaryProtocol;
import fbshade0.org.apache.thrift.protocol.TProtocol;
import fbshade0.org.apache.thrift.transport.TIOStreamTransport;
import org.apache.spark.util.CircularBuffer;
import org.apache.spark.util.RedirectThread;
import org.apache.spark.util.ShutdownHookManager;
import scala.runtime.BoxedUnit;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NativeEngineOperator
        implements SourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final JsonCodec<TaskSource> taskSourceCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;
    private final PlanFragment planFragment;
    private final TableWriteInfo tableWriteInfo;
    private final LocalTaskFactory localTaskFactory;
    private TaskSource taskSource;
    private final boolean isFirstOperator;

    private boolean finished;

    private static final Logger log = Logger.get(NativeEngineOperator.class);

    private static class NativeEngineClient
    {

        final int libraryVersion = 1;
        final byte[] header = "NATIVE".getBytes();
        final String initTimeout = "20s";
        // data bytes buffer (4-bytes Integer)
        private final ByteBuffer bytesBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
        private InputStream input;
        private OutputStream output;

        public NativeEngineClient(InputStream input,
                OutputStream output)
                throws IOException
        {
            this.input = input;
            this.output = output;
        }

        /**
         * Binary protocol initialization for Native Execution.
         */
        private void init()
                throws IOException
        {
            // logInfo(s"NativeEngine protocol init. Library version: $libraryVersion")

            // logInfo(s"Reading NativeEngine init frame from native engine process...")
            byte[] receivedHeaderBytes = new byte[header.length];

            int readBytes = input.read(receivedHeaderBytes);

            if (!Arrays.equals(header, receivedHeaderBytes)) {
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
        private void sendSparkPlan(PrestoTaskSubmit taskSubmit)
                throws IOException
        {
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

        private void writeFrameLength(int frameLength)
                throws IOException
        {
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
    private static class MonitorThread
            extends Thread
    {

        private ProcessBuilder scriptProcess;
        private CircularBuffer stderrBuffer;
        private String tmpDir;
        private File logFile;
        private Optional<org.apache.spark.TaskContext> context;
        //  private TaskContext context;

        public MonitorThread(
                org.apache.spark.TaskContext context,
                ProcessBuilder scriptProcess,
                CircularBuffer stderrBuffer)
                throws InterruptedException
        {
            this.scriptProcess = scriptProcess;
            this.stderrBuffer = stderrBuffer;
            this.tmpDir = isTesting() ? "/tmp" : System.getProperty("java.io.tmpdir");
            this.logFile = new File(System.getenv().getOrDefault(
                    "BUMBLEBEE_CONTAINER_LOG_DIR", tmpDir), "transform_stderr");
            //this.context = Optional.of(context);
            this.context = Optional.empty();
            setDaemon(true);
            setName("Thread-Worker-Monitor");

            String cmd = String.join(" ", scriptProcess.command());
            String headerString = "Script:" + cmd + "\n";
            headerString += "*** Transform stderr log ***";
            logStderrToFile(headerString, false);
        }

        private boolean isTesting()
        {
            return (System.getenv("SPARK_TESTING") != null)
                    || (System.getenv("spark.testing") != null);
        }

        /**
         * A utility method to log the child process's stderr to file.
         */
        private synchronized void logStderrToFile(String lines)
        {
            logStderrToFile(lines, true);
        }

        private synchronized void logStderrToFile(String lines, boolean resetStderrBuffer)
        {
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
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to write '${logFile.getAbsolutePath", e);
                }
            }
        }

        private void logStderrAndSleep()
                throws InterruptedException
        {
            logStderrToFile(stderrBuffer.toString());
            Thread.sleep(5000);    // flush logs every 5 seconds in local-mode
        }

        @Override
        public void run()
        {
            if (context.isPresent()) {
                // Only run the thread if the task if is not completed nor interrupted.
                while (!context.get().isInterrupted() && !context.get().isCompleted()) {
                    try {
                        logStderrAndSleep();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            else {
                while (true) {
                    try {
                        logStderrAndSleep();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @Override
        public void interrupt()
        {
            logStderrToFile(stderrBuffer.toString());
        }
    }

    private static class ResidentNativeEngineReaderThread
            extends Thread
    {

        private volatile Throwable _exception = null;
        private final InputStream inputStream;
        private final ConcurrentHashMap<Long, com.facebook.spark.protocol.TaskResult> tasksResults;

        public ResidentNativeEngineReaderThread(
                InputStream inputStream,
                ConcurrentHashMap<Long, com.facebook.spark.protocol.TaskResult> taskResults)
        {
            this.inputStream = inputStream;
            this.tasksResults = taskResults;
            setName("Thread-ResidentNativeEngine-Reader");
            setDaemon(true);
        }

        /**
         * Contains the exception thrown while reading from the external process.
         */
        public Optional<Throwable> exception()
        {
            if (_exception != null) {
                return Optional.of(_exception);
            }
            else {
                return Optional.empty();
            }
        }

        public static byte[] toByteArray(InputStream input, int size)
                throws IOException
        {
            if (size < 0) {
                throw new IllegalArgumentException("Size must be equal or greater than zero: " + size);
            }
            else if (size == 0) {
                return new byte[0];
            }
            else {
                byte[] data = new byte[size];

                int offset;
                int readed;
                for (offset = 0; offset < size && (readed = input.read(data, offset, size - offset)) != -1; offset += readed) {
                }

                if (offset != size) {
                    throw new IOException("Unexpected readed size. current: " + offset + ", excepted: " + size);
                }
                else {
                    return data;
                }
            }
        }

        private byte[] readBytes(int toRead, String what)
                throws IOException
        {
            return toByteArray(inputStream, toRead);
        }

        /*
        Deserialize the byte streams of the TaskResult thrift object from external process.
        */
        public TaskResult deserializeTaskResult(byte[] bytes)
        {
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            TIOStreamTransport transport = new TIOStreamTransport(byteArrayInputStream);
            return TaskResult.read0(protocolFactory.getProtocol(transport));
        }

        /**
         * Read TaskResult thrift object from transform process.
         * and add it to TasksResults map
         */
        @Override
        public void run()
        {
            while (!Thread.interrupted()) {
                try {
                    // get data length for task result
                    int dataLength =
                            ByteBuffer.wrap(readBytes(Integer.BYTES, "task result data length"))
                                    .order(ByteOrder.nativeOrder()).getInt();

                    // get data bytes for task metric
                    byte[] taskResultBytes = readBytes(dataLength, "task result");
                    TaskResult taskResult = deserializeTaskResult(taskResultBytes);
                    tasksResults.put(taskResult.getTaskId(), taskResult);
                }
                catch (Throwable e) {
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

    public static class ResidentNativeEngine
    {

        private static volatile Optional<Process> process = Optional.empty();
        private static NativeEngineClient nativeEngineClient = null;
        private static Thread monitorThread = null;
        private static ResidentNativeEngineReaderThread readerThread = null;
        private static final CircularBuffer stderrBuffer = new CircularBuffer(102400); // 100 kilobytes
        // map TaskIDs -> TaskResult
        private static final ConcurrentHashMap<Long, TaskResult> tasksResults = new ConcurrentHashMap<Long, TaskResult>();

        public static TaskResult waitAndGetTaskResult(long taskAttemptId)
                throws InterruptedException
        {
            // TODO: use locks on tasksResults keys
            while (tasksResults.get(taskAttemptId) == null) {
                // check for exceptions in the reader thread
                if (!process.get().isAlive() || readerThread.exception().isPresent()) {
                    monitorThread.interrupt(); // flush stderr buffer to file
                    if (process.isPresent() && !process.get().isAlive() && process.get().exitValue() != 0) {
                        String errorReason = !process.get().isAlive() ?
                                "exit code=" + process.get().exitValue() :
                                "readerThread exception: " + readerThread.exception().toString();
                        throw new RuntimeException(errorReason);
                    }
                    else {
                        // This branch is for testing purpose only
                        return new TaskResult.Builder().setTaskId(0).build();
                    }
                }
                Thread.sleep(1000);
            }
            monitorThread.interrupt(); // write NativeEngine's stderr to file

            return tasksResults.remove(taskAttemptId);  // remove result from the map and return
        }

        public static void sendSparkPlan(PrestoTaskSubmit taskSubmit)
                throws IOException
        {
            nativeEngineClient.sendSparkPlan(taskSubmit);
        }

        // stream to read stderr from the NativeEngine process
        private static InputStream getErrorStream()
        {
            return process.get().getErrorStream();
        }

        private static InputStream getInputStream()
        {
            return process.get().getInputStream();
        }

        private static OutputStream getOutputStream()
        {
            return process.get().getOutputStream();
        }

        static void endWord(List<String> buf, StringBuilder curWord)
        {
            buf.add(curWord.toString());
            curWord.setLength(0);
        }

        private static boolean isSpace(char c)
        {
            return " \t\r\n".indexOf(c) != -1;
        }

        private static List<String> splitCommandString(String s)
        {
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
                    }
                    else if (nextChar == '\\') {
                        if (i < s.length() - 1) {
                            // Append the next character directly, because only " and \ may be escaped in
                            // double quotes after the shell's own expansion
                            curWord.append(s.charAt(i + 1));
                            i += 1;
                        }
                    }
                    else {
                        curWord.append(nextChar);
                    }
                }
                else if (inSingleQuote) {
                    if (nextChar == '\'') {
                        inSingleQuote = false;
                    }
                    else {
                        curWord.append(nextChar);
                    }
                    // Backslashes are not treated specially in single quotes
                }
                else if (nextChar == '"') {
                    inWord = true;
                    inDoubleQuote = true;
                }
                else if (nextChar == '\'') {
                    inWord = true;
                    inSingleQuote = true;
                }
                else if (!isSpace(nextChar)) {
                    curWord.append(nextChar);
                    inWord = true;
                }
                else if (inWord && isSpace(nextChar)) {
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

        private static ProcessBuilder getProcessBuilder(String script)
        {
            // Getting script full-path
            File workingDir = new File("/Users/mjdeng/Projects/presto/presto-spark-base/src/test"
                    + "/resources/");
            // File workingDir = new File(SparkFiles.getRootDirectory());
            List<String> args = splitCommandString(script);

            if (!new File(args.get(0)).isAbsolute()) {
                if (new File(workingDir, args.get(0)).exists()) {
                    args.set(0, new File(workingDir, args.get(0)).getAbsolutePath());
                }
            }

            if (!new File(args.get(0)).exists()) {
                throw new RuntimeException("File " + args.get(0) + " does not exist. ");
            }

            // script ProcessBuilder
            ProcessBuilder builder = new ProcessBuilder(args);
            // set working directory
            // builder.directory(workingDir);
            // environment variables
            Map<String, String> env = builder.environment();
            env.put("PATH", env.getOrDefault("PATH", "") + workingDir.getAbsolutePath());
            // unset LD_PRELOAD (prevent inheritance from parent)
            env.remove("LD_PRELOAD");
            // Set spark.executor.cores to allow NativeEngine to use it to set threadPoolSize
            // env.put("spark.executor.cores", "4");
            env.put("spark.executor.cores", "1");

            return builder;
        }

        private static void stop()
                throws IOException, InterruptedException
        {
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

        public static synchronized void startProcess(String script)
                throws IOException, InterruptedException
        {
            if (!process.isPresent()) {
                // launch process
                ProcessBuilder processBuilder = getProcessBuilder(script);

                process = Optional.of(processBuilder.start());

                // To avoid issues caused by large error output, we use a circular buffer to limit the
                // amount of error output (stderr) that we retain.
                new RedirectThread(getErrorStream(), stderrBuffer,
                        "Thread-ResidentNativeEngine-STDERR-Consumer", false).start();

                // periodically log stderr from buffer to file
                monitorThread = new MonitorThread(org.apache.spark.TaskContext.get(), processBuilder, stderrBuffer);
                monitorThread.start();

                // add shutdown hook
                ShutdownHookManager.addShutdownHook(ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY(), new scala.runtime.AbstractFunction0<BoxedUnit>()
                {
                    @Override
                    public BoxedUnit apply()
                    {
                        try {
                            stop();
                        }
                        catch (IOException | InterruptedException e) {
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

    public NativeEngineOperator(PlanNodeId sourceId, OperatorContext operatorContext, JsonCodec<TaskSource> taskSourceCodec, JsonCodec<PlanFragment> planFragmentCodec, JsonCodec<TableWriteInfo> tableWriteInfoCodec, PlanFragment planFragment, TableWriteInfo tableWriteInfo, LocalTaskFactory localTaskFactory, boolean isFirstOperator)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();
        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceJsonCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
        this.planFragment = requireNonNull(planFragment, "planFragment is null");
        this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        this.localTaskFactory = requireNonNull(localTaskFactory, "localTaskFactory is null");
        this.taskSource = null;
        this.finished = false;
        this.isFirstOperator = isFirstOperator;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        return;
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        log.info("Running in ResidentNativeEngine mode");
        String script = Paths.get("src", "test", "resources", "MockNativeEngine.py").toFile().getAbsolutePath();
        try {
            PrestoTaskSubmit prestoTaskSubmit = new PrestoTaskSubmit.Builder().setTaskSources(ImmutableList.of(taskSourceCodec.toJson(taskSource))).setPlanFragment(planFragmentCodec.toJson(planFragment)).setStagingPath("").setTableWriteInfo(tableWriteInfoCodec.toJson(tableWriteInfo)).build();
            // ResidentNativeEngine.startProcess(script + " --resident-mode");
//            ResidentNativeEngine.sendSparkPlan(prestoTaskSubmit);
//            org.apache.spark.TaskContext context = org.apache.spark.TaskContext.get();
//            long taskAttemptId = context != null ? context.taskAttemptId() : 0l;
//            log.info("Waiting for NativeEngine to complete task " + taskAttemptId);
//            TaskResult taskResult = ResidentNativeEngine.waitAndGetTaskResult(taskAttemptId);
            // processTaskResult(taskResult)
            LocalTask localTask = localTaskFactory.createLocalTask(
                    operatorContext.getSession(),
                    operatorContext.getDriverContext().getTaskId(),
                    new InternalNode("node-id", URI.create("http://127.0.0.1/"), new NodeVersion("version"), false),
                    planFragment,
                    ImmutableList.of(taskSource),
                    // ImmutableList.of(),
                    createInitialEmptyOutputBuffers(OutputBuffers.BufferType.PARTITIONED),
                    tableWriteInfo);

            localTask.start();
            boolean isDone = false;
            while (!isDone) {
                if (localTask.getTaskInfo().getTaskStatus().getState().isDone()) {
                    isDone = true;
                }
                Thread.sleep(5000);    // flush logs every 5 seconds in local-mode
            }
        }
        // catch (IOException | InterruptedException e) {
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finished = true;
        return null;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkState(this.taskSource == null, "NativeEngine operator split already set");

        if (finished) {
            return Optional::empty;
        }

        checkState(split.getConnectorSplit() instanceof NativeEngineSplitWrapper, "NativeEngine can only consume the split in NativeEngineSplitWrapper type");
        NativeEngineSplitWrapper nativeEngineSplit = (NativeEngineSplitWrapper) split.getConnectorSplit();
        PlanNodeId planNodeId = nativeEngineSplit.getSourceNodeId();
        this.taskSource = new TaskSource(planNodeId, ImmutableSet.of(new ScheduledSplit(0, planNodeId, new Split(split.getConnectorId(), split.getTransactionHandle(), nativeEngineSplit, split.getLifespan(), split.getSplitContext()))), true);

        Object splitInfo = split.getInfo();
        Map<String, String> infoMap = split.getInfoMap();

        //Make the implicit assumption that if infoMap is populated we can use that instead of the raw object.
        if (infoMap != null && !infoMap.isEmpty()) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(infoMap)));
        }
        else if (splitInfo != null) {
            operatorContext.setInfoSupplier(Suppliers.ofInstance(new SplitOperatorInfo(splitInfo)));
        }

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        if (taskSource == null) {
            finished = true;
        }
    }

    @Override
    public void close()
    {
        systemMemoryContext.setBytes(0);
    }

    private List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }

    public static class NativeEngineOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JsonCodec<TaskSource> taskSourceCodec;
        private final JsonCodec<PlanFragment> planFragmentCodec;
        private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;
        private final PlanFragment planFragment;
        private final TableWriteInfo tableWriteInfo;
        private final LocalTaskFactory localTaskFactory;
        private boolean isFirstOperator = true;
        private boolean closed;

        public NativeEngineOperatorFactory(int operatorId, PlanNodeId planNodeId, JsonCodec<TaskSource> taskSourceCodec, JsonCodec<PlanFragment> planFragmentCodec, JsonCodec<TableWriteInfo> tableWriteInfoCodec, PlanFragment planFragment, TableWriteInfo tableWriteInfo, LocalTaskFactory localTaskFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceJsonCodec is null");
            this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
            this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
            this.planFragment = requireNonNull(planFragment, "planFragment is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
            this.localTaskFactory = requireNonNull(localTaskFactory, "localTaskFactory is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "operator factory is closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NativeEngineOperator.class.getSimpleName());
            SourceOperator operator = new NativeEngineOperator(
                    planNodeId,
                    operatorContext,
                    taskSourceCodec,
                    planFragmentCodec,
                    tableWriteInfoCodec,
                    planFragment,
                    tableWriteInfo,
                    localTaskFactory,
                    isFirstOperator);
            isFirstOperator = false;
            return operator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        public PlanFragment getPlanFragment()
        {
            return planFragment;
        }
    }
}
