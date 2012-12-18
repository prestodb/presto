package com.facebook.presto.event.scribe.client;

import com.facebook.swift.service.RuntimeTTransportException;
import com.google.common.base.Charsets;
import io.airlift.units.DataSize;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAsyncScribeLogger
{
    private LogRecorder logRecorder;
    private Provider<ScribeClient> provider;


    @BeforeMethod
    public void setUp()
            throws Exception
    {
        logRecorder = new LogRecorder();
        provider = new Provider<ScribeClient>()
        {
            @Override
            public ScribeClient get()
            {
                return new MockScribeClient(logRecorder);
            }
        };
    }

    @Test
    public void testSanity()
            throws Exception
    {
        AsyncScribeLogger asyncScribeLogger = new AsyncScribeLogger(10, provider, DataSize.valueOf("1MB"));
        AsyncScribeLogger.FlushTask flushTask = asyncScribeLogger.createFlushTask();

        asyncScribeLogger.log(createLogEntry("cat", "msg"));
        assertTrue(logRecorder.getLoggedMessages().isEmpty());
        assertEquals(logRecorder.getLogCallCount(), 0);

        flushTask.process();

        assertEquals(logRecorder.getLoggedMessages().size(), 1);
        assertEquals(logRecorder.getLogCallCount(), 1);

        asyncScribeLogger.log(createLogEntry("cat", "msg"));
        asyncScribeLogger.log(createLogEntry("cat", "msg"));
        assertEquals(logRecorder.getLoggedMessages().size(), 1);
        assertEquals(logRecorder.getLogCallCount(), 1);

        flushTask.process();

        assertEquals(logRecorder.getLoggedMessages().size(), 3);
        assertEquals(logRecorder.getLogCallCount(), 2);
    }

    @Test
    public void testLogBatching()
            throws Exception
    {
        // Batch sizes targeted for 1 Byte
        AsyncScribeLogger asyncScribeLogger = new AsyncScribeLogger(10, provider, DataSize.valueOf("1B"));
        AsyncScribeLogger.FlushTask flushTask = asyncScribeLogger.createFlushTask();

        asyncScribeLogger.log(createLogEntry("cat", "msg"));
        assertTrue(logRecorder.getLoggedMessages().isEmpty());
        assertEquals(logRecorder.getLogCallCount(), 0);

        flushTask.process();

        assertEquals(logRecorder.getLoggedMessages().size(), 1);
        assertEquals(logRecorder.getLogCallCount(), 1);

        asyncScribeLogger.log(createLogEntry("cat", "msg"));
        asyncScribeLogger.log(createLogEntry("cat", "msg"));

        assertEquals(logRecorder.getLoggedMessages().size(), 1);
        assertEquals(logRecorder.getLogCallCount(), 1);

        flushTask.process();

        // Should be one log call for each message
        assertEquals(logRecorder.getLoggedMessages().size(), 3);
        assertEquals(logRecorder.getLogCallCount(), 3);
    }

    @Test
    public void testBufferEviction()
            throws Exception
    {
        AsyncScribeLogger asyncScribeLogger = new AsyncScribeLogger(2, provider, DataSize.valueOf("1MB"));
        AsyncScribeLogger.FlushTask flushTask = asyncScribeLogger.createFlushTask();

        // Log 3 messages without flushing
        asyncScribeLogger.log(createLogEntry("cat1", "msg"));
        asyncScribeLogger.log(createLogEntry("cat2", "msg"));
        asyncScribeLogger.log(createLogEntry("cat3", "msg"));
        assertTrue(logRecorder.getLoggedMessages().isEmpty());
        assertEquals(logRecorder.getLogCallCount(), 0);

        flushTask.process();

        // Only last two messages should appear since the first got evicted
        assertEquals(logRecorder.getLoggedMessages().size(), 2);
        assertEquals(new String(logRecorder.getLoggedMessages().get(0).getCategory(), Charsets.UTF_8), "cat2");
        assertEquals(new String(logRecorder.getLoggedMessages().get(1).getCategory(), Charsets.UTF_8), "cat3");
        assertEquals(logRecorder.getLogCallCount(), 1);
    }

    @Test
    public void testFailingClient()
            throws Exception
    {
        final AtomicBoolean first = new AtomicBoolean(true);
        Provider<ScribeClient> fixedProvider = new Provider<ScribeClient>()
        {
            @Override
            public ScribeClient get()
            {
                // Have only the first ScribeClient fail
                if (first.get()) {
                    MockScribeClient mockScribeClient = new MockScribeClient(logRecorder);
                    mockScribeClient.setException(true);
                    first.set(false);
                    return mockScribeClient;
                }
                else {
                    return new MockScribeClient(logRecorder);
                }
            }
        };

        // Buffer queue only allows one message
        AsyncScribeLogger asyncScribeLogger = new AsyncScribeLogger(1, fixedProvider, DataSize.valueOf("1MB"));
        AsyncScribeLogger.FlushTask flushTask = asyncScribeLogger.createFlushTask();

        // The message should be dropped in the failed buffer
        asyncScribeLogger.log(createLogEntry("cat1", "msg"));
        flushTask.process();

        assertEquals(logRecorder.getLoggedMessages().size(), 0);
        assertEquals(logRecorder.getLogCallCount(), 0);

        asyncScribeLogger.log(createLogEntry("cat2", "msg"));

        flushTask.process();

        // Both messages should be present
        assertEquals(logRecorder.getLoggedMessages().size(), 2);
        assertEquals(logRecorder.getLogCallCount(), 2);
    }

    private static LogEntry createLogEntry(String category, String message)
    {
        return new LogEntry(category, message);
    }

    private static class MockScribeClient
            implements ScribeClient
    {
        private final LogRecorder logRecorder;

        private boolean closed = false;
        private boolean fail = false;
        private boolean exception = false;

        private MockScribeClient(LogRecorder logRecorder) {
            this.logRecorder = checkNotNull(logRecorder, "logRecorder is null");
        }

        @Override
        public ResultCode log(List<LogEntry> messages)
        {
            checkState(!closed, "already closed");
            if (exception) {
                throw new RuntimeTTransportException("failed", null);
            }
            if (fail) {
                return ResultCode.TRY_LATER;
            }
            logRecorder.record(messages);
            return ResultCode.OK;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        public void setException(boolean exception)
        {
            this.exception = exception;
        }

        public void setFail(boolean fail)
        {
            this.fail = fail;
        }
    }

    private static class LogRecorder
    {
        private final List<LogEntry> loggedMessages = new ArrayList<>();
        private int logCallCount;

        public void record(List<LogEntry> entries)
        {
            loggedMessages.addAll(entries);
            logCallCount += 1;
        }

        public List<LogEntry> getLoggedMessages()
        {
            return loggedMessages;
        }

        public int getLogCallCount()
        {
            return logCallCount;
        }
    }
}
