package com.facebook.presto.cli;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.cli.AsyncBufferedOutputStream.BufferTask.Type.DATA;
import static com.facebook.presto.cli.AsyncBufferedOutputStream.BufferTask.Type.DATA_FLUSH;
import static com.facebook.presto.cli.AsyncBufferedOutputStream.BufferTask.Type.DONE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class AsyncBufferedOutputStream
        extends OutputStream
{
    private final ExecutorService executor = newSingleThreadExecutor(threadsNamed("async-output-%d"));
    private final BlockingQueue<BufferTask> buffer = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean failure = new AtomicBoolean();
    private final AtomicReference<IOException> exception = new AtomicReference<>();
    private final OutputStream out;

    private AsyncBufferedOutputStream(OutputStream out)
    {
        this.out = checkNotNull(out, "out is null");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        write(new byte[] {(byte) b});
    }

    @Override
    public void write(byte[] b)
            throws IOException
    {
        checkException();
        buffer.add(new BufferTask(b, DATA));
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        write(Arrays.copyOfRange(b, off, off + len));
    }

    @Override
    public void flush()
            throws IOException
    {
        checkState(!closed.get(), "already closed");
        BufferTask flush = new BufferTask(null, DATA_FLUSH);
        buffer.add(flush);
        flush.waitForCompletion();
        checkException();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed.getAndSet(true)) {
            return;
        }
        BufferTask done = new BufferTask(null, DONE);
        buffer.add(done);
        try {
            done.waitForCompletion();
        }
        finally {
            executor.shutdown();
        }
        checkException();
    }

    private void checkException()
            throws IOException
    {
        if (exception.get() != null) {
            throw exception.getAndSet(null);
        }
    }

    private void start()
    {
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                while (true) {
                    try {
                        if (consumeTask()) {
                            break;
                        }
                    }
                    catch (IOException e) {
                        exception.set(e);
                        failure.set(true);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        exception.set(new InterruptedIOException());
                        failure.set(true);
                    }
                }
            }

            private boolean consumeTask()
                    throws InterruptedException, IOException
            {
                BufferTask task = buffer.take();
                if (task.getType() == DATA) {
                    if (!failure.get()) {
                        out.write(task.getData());
                    }
                }
                else {
                    try {
                        out.flush();
                    }
                    finally {
                        try {
                            if (task.getType() == DONE) {
                                out.close();
                                return true;
                            }
                        }
                        finally {
                            task.signalCompletion(null);
                        }
                    }
                }
                return false;
            }
        });
    }

    /**
     * An enumerated type for tasks send to the BlockingQueue buffer
     */
    static class BufferTask
    {
        private final SettableFuture<Void> future = SettableFuture.create();
        private final byte[] data;
        private final Type type;

        public enum Type
        {
            DATA_FLUSH,
            DONE,
            DATA
        }

        private BufferTask(byte[] data, Type task)
        {
            this.data = data;
            this.type = task;
        }

        public void waitForCompletion()
                throws InterruptedIOException
        {
            try {
                future.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
            catch (ExecutionException e) {
                throw new AssertionError(e);
            }
        }

        public void signalCompletion(Void value)
        {
            future.set(value);
        }

        private byte[] getData()
        {
            checkState(type == DATA, "not a DATA task: %s", type);
            return data;
        }

        public Type getType()
        {
            return type;
        }
    }

    public static OutputStream create(OutputStream out)
    {
        AsyncBufferedOutputStream async = new AsyncBufferedOutputStream(out);
        async.start();
        return new BufferedOutputStream(async);
    }

    private static ThreadFactory threadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }
}
