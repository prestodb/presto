package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
abstract class BufferedOutputHandler
        implements OutputHandler
{
    private static final Duration MAX_BUFFER_TIME = new Duration(3, TimeUnit.SECONDS);
    private static final int MAX_BUFFERED_ROWS = 10_000;

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("flusher-%s"));
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<List<?>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);

    @GuardedBy("this") private Future<?> flusher;
    @GuardedBy("this") private IOException flushException;

    @GuardedBy("this") protected final List<String> fieldNames;
    @GuardedBy("this") protected final Writer writer;

    protected BufferedOutputHandler(List<String> fieldNames, Writer writer)
    {
        this.fieldNames = ImmutableList.copyOf(checkNotNull(fieldNames, "fieldNames is null"));
        this.writer = checkNotNull(writer, "writer is null");
    }

    @Override
    public final synchronized void processRow(List<?> values)
            throws IOException
    {
        checkFlushException();
        startFlusher();
        checkArgument(fieldNames.size() == values.size(), "field names size does not match row size");
        rowBuffer.add(values);
        if (rowBuffer.size() == MAX_BUFFERED_ROWS) {
            doFlush();
        }
    }

    @Override
    public final synchronized void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            executor.shutdownNow();
            checkFlushException();
            doFlush();
            finish();
        }
    }

    @GuardedBy("this")
    protected abstract void flush(List<List<?>> rowBuffer)
            throws IOException;

    @GuardedBy("this")
    protected abstract void finish()
            throws IOException;

    private synchronized void doFlush()
            throws IOException
    {
        flush(unmodifiableList(rowBuffer));
        rowBuffer.clear();
        stopFlusher();
    }

    private synchronized void startFlusher()
    {
        flusher = executor.schedule(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    doFlush();
                }
                catch (IOException e) {
                    flushException = e;
                }
            }
        }, (long) MAX_BUFFER_TIME.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void stopFlusher()
    {
        if (flusher != null) {
            flusher.cancel(true);
            flusher = null;
        }
    }

    private synchronized void checkFlushException()
            throws IOException
    {
        if (flushException != null) {
            throw flushException;
        }
    }

    private static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
    }
}
