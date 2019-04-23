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
package com.facebook.presto.raptorx;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.raptorx.metadata.BucketManager;
import com.facebook.presto.raptorx.metadata.MetadataWriter;
import com.facebook.presto.raptorx.metadata.NodeSupplier;
import com.facebook.presto.raptorx.metadata.RaptorNodeSupplier;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.storage.StorageConfig;
import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.bootstrap.LifeCycleManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.raptorx.RaptorTableProperties.COMPRESSION_TYPE_PROPERTY;
import static com.facebook.presto.raptorx.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRaptorConnector
{
    private static final int MAX_CHUNK_ROWS = 1000;
    private File dataDir;
    private RaptorConnector connector;
    private TestingDatabase database;
    private TestingEnvironment testingEnvironment;
    private TransactionManager transactionManager;
    private TransactionWriter transactionWriter;
    private StorageManager storageManager;
    private MetadataWriter metadataWriter;
    private RaptorTableProperties raptorTableProperties;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        database = new TestingDatabase();
        new SchemaCreator(database).create();
        testingEnvironment = new TestingEnvironment(database);
        transactionManager = new TransactionManager(testingEnvironment.getMetadata());
        dataDir = Files.createTempDir();
        transactionWriter = testingEnvironment.getTransactionWriter();
        storageManager = createOrcStorageManager(database, dataDir, MAX_CHUNK_ROWS);
        metadataWriter = testingEnvironment.getMetadataWriter();
        raptorTableProperties = new RaptorTableProperties(testingEnvironment.getTypeManager());

        NodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = new RaptorNodeSupplier(nodeManager, testingEnvironment.getNodeIdCache());
        BucketManager bucketManager = new BucketManager(database, testingEnvironment.getTypeManager());
        StorageConfig config = new StorageConfig();
        connector = new RaptorConnector(
                new LifeCycleManager(ImmutableList.of(), null),
                transactionManager,
                nodeSupplier,
                bucketManager,
                transactionWriter,
                new RaptorSplitManager(nodeSupplier, transactionManager),
                new RaptorPageSourceProvider(storageManager),
                new RaptorPageSinkProvider(storageManager,
                        new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                        config),
                new RaptorNodePartitioningProvider(nodeSupplier),
                new RaptorSessionProperties(config),
                raptorTableProperties,
                ImmutableSet.of(),
                ImmutableSet.of(),
                nodeManager);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (database != null) {
            database.close();
        }
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testMaintenanceBlocked()
    {
        long tableId1 = createTable("test1");
        long tableId2 = createTable("test2");

        assertFalse(metadataWriter.isMaintenanceBlocked(tableId1));
        assertFalse(metadataWriter.isMaintenanceBlocked(tableId2));

        // begin for table1
        ConnectorTransactionHandle txn1 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle1 = getTableHandle(connector.getMetadata(txn1), "test1");
        connector.getMetadata(txn1).beginInsert(SESSION, handle1);

        assertFalse(metadataWriter.isMaintenanceBlocked(tableId1));

        connector.getMetadata(txn1).beginDelete(SESSION, handle1);
        assertTrue(metadataWriter.isMaintenanceBlocked(tableId1));
        assertFalse(metadataWriter.isMaintenanceBlocked(tableId2));

        // begin for table2
        ConnectorTransactionHandle txn2 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle2 = getTableHandle(connector.getMetadata(txn2), "test2");
        connector.getMetadata(txn2).beginDelete(SESSION, handle2);

        assertTrue(metadataWriter.isMaintenanceBlocked(tableId1));
        assertTrue(metadataWriter.isMaintenanceBlocked(tableId2));

        // begin another for table1
        ConnectorTransactionHandle txn3 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle3 = getTableHandle(connector.getMetadata(txn3), "test1");
        connector.getMetadata(txn3).beginDelete(SESSION, handle3);

        assertTrue(metadataWriter.isMaintenanceBlocked(tableId1));
        assertTrue(metadataWriter.isMaintenanceBlocked(tableId2));

        // commit first for table1
        connector.commit(txn1);

        assertTrue(metadataWriter.isMaintenanceBlocked(tableId1));
        assertTrue(metadataWriter.isMaintenanceBlocked(tableId2));

        // rollback second for table1
        connector.rollback(txn3);

        assertFalse(metadataWriter.isMaintenanceBlocked(tableId1));
        assertTrue(metadataWriter.isMaintenanceBlocked(tableId2));

        // commit for table2
        connector.commit(txn2);

        assertFalse(metadataWriter.isMaintenanceBlocked(tableId1));
        assertFalse(metadataWriter.isMaintenanceBlocked(tableId2));
    }

    @Test
    public void testMaintenanceUnblockedOnStart()
    {
        long tableId = createTable("test");

        assertFalse(metadataWriter.isMaintenanceBlocked(tableId));
        metadataWriter.blockMaintenance(tableId);
        assertTrue(metadataWriter.isMaintenanceBlocked(tableId));

        connector.start();

        assertFalse(metadataWriter.isMaintenanceBlocked(tableId));
    }

    private long createTable(String name)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        if (!connector.getMetadata(transaction).schemaExists(SESSION, "test")) {
            connector.getMetadata(transaction).createSchema(SESSION, "test", ImmutableMap.of());
        }
        connector.getMetadata(transaction).createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("test", name),
                        ImmutableList.of(new ColumnMetadata("id", BIGINT)),
                        ImmutableMap.of(COMPRESSION_TYPE_PROPERTY, CompressionType.ZSTD)),
                false);
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
