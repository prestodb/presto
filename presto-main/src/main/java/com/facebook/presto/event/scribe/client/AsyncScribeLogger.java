package com.facebook.presto.event.scribe.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.event.scribe.client.ReusableScribeClient.makeReusableClient;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class AsyncScribeLogger
{
    private final static Logger log = Logger.get(AsyncScribeLogger.class);

    private static final Duration ERROR_BACKOFF = Duration.valueOf("4s");
    private static final Duration POLL_WAIT = Duration.valueOf("1s");

    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ExecutorService executorService = newSingleThreadExecutor(threadsNamed("scribe-logger-%d"));
    private final BlockingQueue<LogEntry> queue;
    private final ReusableScribeClient scribeClient;
    private final DataSize maxBatchSize;

    public AsyncScribeLogger(int maxQueueLength, Provider<ScribeClient> scribeClientProvider, DataSize maxBatchSize)
    {
        checkArgument(maxQueueLength > 0, "maxQueueLength must be greater than zero");
        checkNotNull(scribeClientProvider, "scribeClientProvider is null");
        checkNotNull(maxBatchSize, "maxBatchSize is null");
        checkArgument(maxBatchSize.getValue() > 0, "maxBatchSize must be greater than zero");
        queue = new LinkedBlockingQueue<>(maxQueueLength);
        scribeClient = makeReusableClient(scribeClientProvider);
        this.maxBatchSize = maxBatchSize;
    }

    @Inject
    public AsyncScribeLogger(ScribeClientProvider scribeClientProvider, ScribeClientConfiguration clientConfiguration)
    {
        this(clientConfiguration.getMaxQueueLength(), scribeClientProvider, clientConfiguration.getMaxBatchSize());
    }

    @PostConstruct
    public void start()
    {
        executorService.execute(createFlushTask());
    }

    @PreDestroy
    public void stop()
    {
        shutdown.set(true);
        executorService.shutdown();
    }

    public void log(LogEntry logEntry)
    {
        while (!queue.offer(logEntry)) {
            // Remove an entry before inserting if capacity was exceeded
            log.debug("Buffer capacity exceeded. Dropping oldest entries to make room");
            queue.poll();
        }
    }

    @VisibleForTesting
    FlushTask createFlushTask()
    {
        return new FlushTask();
    }

    @VisibleForTesting
    @NotThreadSafe
    class FlushTask
            implements Runnable
    {
        private List<LogEntry> failedBatch;

        @Override
        public void run()
        {
            try {
                while (!shutdown.get()) {
                    try {
                        if (!process()) {
                            // Sleep to backoff on any downstream failures
                            TimeUnit.MILLISECONDS.sleep((long) ERROR_BACKOFF.toMillis());
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Flush task interrupted");
                        return;
                    }
                    catch (Exception e) {
                        log.warn(e, "Unexpected flush task exception");
                    }
                }
            } finally {
                scribeClient.close();
            }
        }

        @VisibleForTesting
        boolean process()
                throws InterruptedException
        {
            // Always try the failed batch first (if it exists)
            if (failedBatch != null) {
                if (!flushToScribe(failedBatch)) {
                    return false;
                }
                failedBatch = null;
            }

            // Wait for messages (with timeout)
            BatchBuilder batchBuilder = new BatchBuilder();
            LogEntry logEntry = queue.poll((long) POLL_WAIT.toMillis(), TimeUnit.MILLISECONDS);
            if (logEntry == null) {
                // No messages found before timeout
                return true;
            }

            // Fill up batch and log
            do {
                batchBuilder.add(logEntry);
                if (batchBuilder.getSize().toBytes() >= maxBatchSize.toBytes()) {
                    if (!flushToScribe(batchBuilder.build())) {
                        return false;
                    }
                    batchBuilder = new BatchBuilder();
                }
                logEntry = queue.poll();
            }
            while (logEntry != null);

            // Flush anything remaining in the BatchBuilder
            if (!batchBuilder.isEmpty()) {
                if (!flushToScribe(batchBuilder.build())) {
                    return false;
                }
            }
            return true;
        }

        private boolean flushToScribe(List<LogEntry> logEntries)
        {
            try {
                ResultCode resultCode = scribeClient.log(logEntries);
                switch (resultCode) {
                    case OK:
                        return true;
                    case TRY_LATER:
                        log.warn("Scribe log returned TRY_LATER");
                        break;
                    default:
                        throw new AssertionError("Unknown ResultCode: " + resultCode);
                }
            }
            catch (Exception e) {
                log.warn("Failed to log to Scribe: %s", e.getMessage());
            }

            // Set failedBatch so that it can be retried on next attempt
            failedBatch = logEntries;
            return false;
        }
    }

    @NotThreadSafe
    private static class BatchBuilder
    {
        private long numBytes;
        private final List<LogEntry> logEntries = new ArrayList<>();

        public void add(LogEntry logEntry)
        {
            logEntries.add(logEntry);
            numBytes += logEntry.getCategory().length + logEntry.getMessage().length;
        }

        public boolean isEmpty()
        {˜
            return logEntries.isEmpty();
        }

        public DataSize getSize()
        {
            return new DataSize(numBytes, DataSize.Unit.BYTE);
        }

        public List<LogEntry> build()
        {
            return ImmutableList.copyOf(logEntries);
        }
    }
}
