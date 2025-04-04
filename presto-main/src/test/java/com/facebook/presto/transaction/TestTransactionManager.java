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

import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTransactionManager
{
    private static final String CATALOG_NAME = "test_catalog";
    private static final ConnectorId CONNECTOR_ID = new ConnectorId(CATALOG_NAME);
    private static final ConnectorId SYSTEM_TABLES_ID = createSystemTablesConnectorId(CONNECTOR_ID);
    private static final ConnectorId INFORMATION_SCHEMA_ID = createInformationSchemaConnectorId(CONNECTOR_ID);
    private final ExecutorService finishingExecutor = newCachedThreadPool(daemonThreadsNamed("transaction-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        finishingExecutor.shutdownNow();
    }

    @Test
    public void testTransactionWorkflow()
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            CatalogManager catalogManager = new CatalogManager();
            TransactionManager transactionManager = InMemoryTransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), catalogManager, finishingExecutor);

            Connector c1 = new TpchConnectorFactory().create(CATALOG_NAME, ImmutableMap.of(), new TestingConnectorContext());
            registerConnector(catalogManager, transactionManager, CATALOG_NAME, CONNECTOR_ID, c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG_NAME).get().getMetadata();
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CONNECTOR_ID));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of(CONNECTOR_ID, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            getFutureValue(transactionManager.asyncCommit(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testAbortedTransactionWorkflow()
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            CatalogManager catalogManager = new CatalogManager();
            TransactionManager transactionManager = InMemoryTransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), catalogManager, finishingExecutor);

            Connector c1 = new TpchConnectorFactory().create(CATALOG_NAME, ImmutableMap.of(), new TestingConnectorContext());
            registerConnector(catalogManager, transactionManager, CATALOG_NAME, CONNECTOR_ID, c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG_NAME).get().getMetadata();
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CONNECTOR_ID));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of(CONNECTOR_ID, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testFailedTransactionWorkflow()
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            CatalogManager catalogManager = new CatalogManager();
            TransactionManager transactionManager = InMemoryTransactionManager.create(new TransactionManagerConfig(), executor.getExecutor(), catalogManager, finishingExecutor);

            Connector c1 = new TpchConnectorFactory().create(CATALOG_NAME, ImmutableMap.of(), new TestingConnectorContext());
            registerConnector(catalogManager, transactionManager, CATALOG_NAME, CONNECTOR_ID, c1);

            TransactionId transactionId = transactionManager.beginTransaction(false);

            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            ConnectorMetadata metadata = transactionManager.getOptionalCatalogMetadata(transactionId, CATALOG_NAME).get().getMetadata();
            metadata.listSchemaNames(TEST_SESSION.toConnectorSession(CONNECTOR_ID));
            transactionInfo = transactionManager.getTransactionInfo(transactionId);
            assertEquals(transactionInfo.getConnectorIds(), ImmutableList.of(CONNECTOR_ID, INFORMATION_SCHEMA_ID, SYSTEM_TABLES_ID));
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            transactionManager.fail(transactionId);
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            try {
                transactionManager.getCatalogMetadata(transactionId, CONNECTOR_ID);
                fail();
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), TRANSACTION_ALREADY_ABORTED.toErrorCode());
            }
            assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

            getFutureValue(transactionManager.asyncAbort(transactionId));

            assertTrue(transactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    @Test
    public void testExpiration()
            throws Exception
    {
        try (IdleCheckExecutor executor = new IdleCheckExecutor()) {
            InMemoryTransactionManager inMemoryTransactionManager = (InMemoryTransactionManager) InMemoryTransactionManager.create(
                    new TransactionManagerConfig()
                            .setIdleTimeout(new Duration(1, TimeUnit.MILLISECONDS))
                            .setIdleCheckInterval(new Duration(5, TimeUnit.MILLISECONDS)),
                    executor.getExecutor(),
                    new CatalogManager(),
                    finishingExecutor);

            TransactionId transactionId = inMemoryTransactionManager.beginTransaction(false);

            assertEquals(inMemoryTransactionManager.getAllTransactionInfos().size(), 1);
            TransactionInfo transactionInfo = inMemoryTransactionManager.getTransactionInfo(transactionId);
            assertFalse(transactionInfo.isAutoCommitContext());
            assertTrue(transactionInfo.getConnectorIds().isEmpty());
            assertFalse(transactionInfo.getWrittenConnectorId().isPresent());

            inMemoryTransactionManager.trySetInactive(transactionId);
            TimeUnit.MILLISECONDS.sleep(100);
            // make sure it is cleaned up
            inMemoryTransactionManager.cleanUpExpiredTransactions();
            assertTrue(inMemoryTransactionManager.getAllTransactionInfos().isEmpty());
        }
    }

    private static void registerConnector(
            CatalogManager catalogManager,
            TransactionManager transactionManager,
            String catalogName,
            ConnectorId connectorId,
            Connector connector)
    {
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        MetadataManager metadata = MetadataManager.createTestMetadataManager(catalogManager);

        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, new AllowAllAccessControl(), ImmutableList.of()),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId))));
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
        {
            executorService.shutdownNow();
        }
    }
}
