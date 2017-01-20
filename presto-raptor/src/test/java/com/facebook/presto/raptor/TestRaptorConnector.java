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
package com.facebook.presto.raptor;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.testing.TestingNodeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.bootstrap.LifeCycleManager;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRaptorConnector
{
    private Handle dummyHandle;
    private MetadataDao metadataDao;
    private File dataDir;
    private RaptorConnector connector;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(typeRegistry));
        dummyHandle = dbi.open();
        metadataDao = dbi.onDemand(MetadataDao.class);
        createTablesWithRetry(dbi);
        dataDir = Files.createTempDir();

        RaptorConnectorId connectorId = new RaptorConnectorId("test");
        NodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        ShardManager shardManager = createShardManager(dbi);
        StorageManager storageManager = createOrcStorageManager(dbi, dataDir);
        StorageManagerConfig config = new StorageManagerConfig().setDataDirectory(dataDir);
        connector = new RaptorConnector(
                new LifeCycleManager(ImmutableList.of(), null),
                new TestingNodeManager(),
                new RaptorMetadataFactory(connectorId, dbi, shardManager),
                new RaptorSplitManager(connectorId, nodeSupplier, shardManager, false),
                new RaptorPageSourceProvider(storageManager),
                new RaptorPageSinkProvider(storageManager, new PagesIndexPageSorter(), config),
                new RaptorNodePartitioningProvider(nodeSupplier),
                new RaptorSessionProperties(config),
                new RaptorTableProperties(typeRegistry),
                ImmutableSet.of(),
                dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        dummyHandle.close();
        deleteRecursively(dataDir);
    }

    @Test
    public void testMaintenanceBlocked()
    {
        long tableId1 = createTable("test1");
        long tableId2 = createTable("test2");

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // begin delete for table1
        ConnectorTransactionHandle txn1 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle1 = getTableHandle(connector.getMetadata(txn1), "test1");
        connector.getMetadata(txn1).beginDelete(SESSION, handle1);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // begin delete for table2
        ConnectorTransactionHandle txn2 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle2 = getTableHandle(connector.getMetadata(txn2), "test2");
        connector.getMetadata(txn2).beginDelete(SESSION, handle2);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // begin another delete for table1
        ConnectorTransactionHandle txn3 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle3 = getTableHandle(connector.getMetadata(txn3), "test1");
        connector.getMetadata(txn3).beginDelete(SESSION, handle3);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // commit first delete for table1
        connector.commit(txn1);

        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // rollback second delete for table1
        connector.rollback(txn3);

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId2));

        // commit delete for table2
        connector.commit(txn2);

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId1));
        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId2));
    }

    @Test
    public void testMaintenanceUnblockedOnStart()
            throws Exception
    {
        long tableId = createTable("test");

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId));
        metadataDao.blockMaintenance(tableId);
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId));

        connector.start();

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId));
    }

    private long createTable(String name)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        connector.getMetadata(transaction).createTable(SESSION, new ConnectorTableMetadata(
                new SchemaTableName("test", name),
                ImmutableList.of(new ColumnMetadata("id", BIGINT))));
        connector.commit(transaction);

        transaction = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle tableHandle = getTableHandle(connector.getMetadata(transaction), name);
        connector.commit(transaction);
        return ((RaptorTableHandle) tableHandle).getTableId();
    }

    private static ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, String name)
    {
        return metadata.getTableHandle(SESSION, new SchemaTableName("test", name));
    }
}
