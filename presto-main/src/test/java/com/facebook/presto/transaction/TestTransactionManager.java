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
package com.facebook.presto.transaction;

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class TestTransactionManager
{
    private final ExecutorService finishingExecutor = newCachedThreadPool(daemonThreadsNamed("transaction-%s"));

    @AfterClass
    public void tearDown()
            throws Exception
    {
        finishingExecutor.shutdownNow();
    }

    @Test
    public void testTransactionWorkflow()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector c1 = new TpchConnectorFactory(new InMemoryNodeManager()).create("c1", ImmutableMap.of());
            transactionManager.addConnector("c1", c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            Assert.assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertFalse(transactionInfo.isAutoCommitContext());
            Assert.assertTrue(transactionInfo.getConnectorIds().isEmpty());
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, "c1");
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession("c1"));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of("c1"));
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.asyncCommit(transactionId).join();

            Assert.assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testAbortedTransactionWorkflow()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector c1 = new TpchConnectorFactory(new InMemoryNodeManager()).create("c1", ImmutableMap.of());
            transactionManager.addConnector("c1", c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            Assert.assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertFalse(transactionInfo.isAutoCommitContext());
            Assert.assertTrue(transactionInfo.getConnectorIds().isEmpty());
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, "c1");
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession("c1"));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of("c1"));
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.asyncAbort(transactionId).join();

            Assert.assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testFailedTransactionWorkflow()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), finishingExecutor);

            Connector c1 = new TpchConnectorFactory(new InMemoryNodeManager()).create("c1", ImmutableMap.of());
            transactionManager.addConnector("c1", c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            Assert.assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertFalse(transactionInfo.isAutoCommitContext());
            Assert.assertTrue(transactionInfo.getConnectorIds().isEmpty());
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getMetadata(transactionId, "c1");
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession("c1"));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of("c1"));
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.fail(transactionId);
            Assert.assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            try {
                transactionManager.getMetadata(transactionId, "c1");
                Assert.fail();
            }
            catch (PrestoException e) {
                Assert.assertEquals(e.getErrorCode(), TRANSACTION_ALREADY_ABORTED.toErrorCode());
            }
            Assert.assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            transactionManager.asyncAbort(transactionId).join();

            Assert.assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testExpiration()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            TransactionManager transactionManager = TransactionManager.create(
                    new TransactionManagerConfig()
                            .setIdleTimeout(new Duration(1, TimeUnit.MILLISECONDS))
                            .setIdleCheckInterval(new Duration(5, TimeUnit.MILLISECONDS)),
                    executor.getExecutor(),
                    finishingExecutor);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            Assert.assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            Assert.assertFalse(transactionInfo.isAutoCommitContext());
            Assert.assertTrue(transactionInfo.getConnectorIds().isEmpty());
            Assert.assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.trySetInactive(transactionId);
            TimeUnit.MILLISECONDS.sleep(100);

            Assert.assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    private static class IdleCheckExecutor
            implements Closeable
    {
        private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("idle-check"));

        public ScheduledExecutorService getExecutor()
        {
            return executorService;
        }

        @Override
        public void close()
                throws IOException
        {
            executorService.shutdownNow();
        }
    }
}
