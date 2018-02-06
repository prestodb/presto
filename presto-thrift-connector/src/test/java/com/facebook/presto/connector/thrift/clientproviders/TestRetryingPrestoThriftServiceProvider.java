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
package com.facebook.presto.connector.thrift.clientproviders;

import com.facebook.presto.connector.thrift.ThriftConnectorConfig;
import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableColumnSet;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.location.HostLocationHandle;
import com.facebook.presto.connector.thrift.location.SimpleHostLocationHandle;
import com.facebook.presto.connector.thrift.tracetoken.ThriftTraceToken;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.HostAddress.fromParts;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestRetryingPrestoThriftServiceProvider
{
    private ListeningScheduledExecutorService retryExecutor;

    @BeforeClass
    void init()
    {
        retryExecutor = listeningDecorator(newScheduledThreadPool(2, threadsNamed("test-thrift-retry-driver-%s")));
    }

    @AfterClass(alwaysRun = true)
    void shutdown()
    {
        try {
            if (retryExecutor != null) {
                retryExecutor.shutdownNow();
            }
        }
        catch (Exception ignore) {
        }
    }

    @Test
    void testRetryService()
    {
        TestingConnectedThriftServiceProvider testingServiceProvider = new TestingConnectedThriftServiceProvider(2);
        ThriftConnectorConfig config = new ThriftConnectorConfig()
                .setMaxRetryAttempts(3)
                .setMinRetrySleepTime(new Duration(10, MILLISECONDS))
                .setMaxRetrySleepTime(new Duration(20, MILLISECONDS));

        RetryingPrestoThriftServiceProvider retryingServiceProvider =
                new RetryingPrestoThriftServiceProvider(testingServiceProvider, retryExecutor, config);

        retryingServiceProvider.runOnAnyHost(PrestoThriftService::listSchemaNames);

        assertEquals(testingServiceProvider.getOpenServiceCount(), 3, "Must open thrift connections for 3 times");
        assertEquals(testingServiceProvider.getCloseServiceCount(), 3, "Must close all 3 opened connections");
    }

    private static class TestingConnectedThriftServiceProvider
            implements ConnectedThriftServiceProvider
    {
        private static final PrestoThriftServiceException RETRYABLE_EXCEPTION = new PrestoThriftServiceException("Mocking a retryable thrift failure", true);
        private static final HostLocationHandle DUMMY_HOST_LOCATION_HANDLE = new SimpleHostLocationHandle(fromParts("test_host", 1234));

        private final AtomicInteger openServiceCount = new AtomicInteger();
        private final AtomicInteger closeServiceCount = new AtomicInteger();
        private final AtomicInteger pendingFailuresToGenerate;

        public TestingConnectedThriftServiceProvider(int pendingFailureToGenerate)
        {
            this.pendingFailuresToGenerate = new AtomicInteger(pendingFailureToGenerate);
        }

        public int getOpenServiceCount()
        {
            return openServiceCount.get();
        }

        public int getCloseServiceCount()
        {
            return closeServiceCount.get();
        }

        @Override
        public ConnectedThriftService anyHostClient(Optional<ThriftTraceToken> traceToken)
        {
            openServiceCount.getAndIncrement();
            return new TestingConnectedThriftService();
        }

        @Override
        public ConnectedThriftService selectedHostClient(List<HostAddress> hosts, Optional<ThriftTraceToken> traceToken)
        {
            openServiceCount.getAndIncrement();
            return new TestingConnectedThriftService();
        }

        private final class TestingConnectedThriftService
                implements ConnectedThriftService
        {
            TestingConnectedThriftService() {}

            void failIfConfigured()
            {
                if (pendingFailuresToGenerate.getAndDecrement() > 0) {
                    throw RETRYABLE_EXCEPTION;
                }
            }

            @Override
            public HostLocationHandle getHostLocationHandle()
            {
                return DUMMY_HOST_LOCATION_HANDLE;
            }

            @Override
            public List<String> listSchemaNames()
                    throws PrestoThriftServiceException
            {
                failIfConfigured();
                return ImmutableList.of();
            }

            @Override
            public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
                    throws PrestoThriftServiceException
            {
                failIfConfigured();
                return ImmutableList.of();
            }

            @Override
            public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
                    throws PrestoThriftServiceException
            {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public ListenableFuture<PrestoThriftSplitBatch> getSplits(PrestoThriftSchemaTableName schemaTableName, PrestoThriftNullableColumnSet desiredColumns, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken)
                    throws PrestoThriftServiceException
            {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(PrestoThriftSchemaTableName schemaTableName, List<String> indexColumnNames, List<String> outputColumnNames, PrestoThriftPageResult keys, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken)
                    throws PrestoThriftServiceException
            {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
                    throws PrestoThriftServiceException
            {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void close()
            {
                closeServiceCount.getAndIncrement();
            }
        }
    }
}
