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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.plugin.base.security.AllowAllAccessControl;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.raptor.storage.organization.TemporalFunction;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.raptor.RaptorTableProperties.TABLE_SUPPORTS_DELTA_DELETE;
import static com.facebook.presto.raptor.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.raptor.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
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
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new TableColumn.Mapper(functionAndTypeManager));
        dummyHandle = dbi.open();
        metadataDao = dbi.onDemand(MetadataDao.class);
        createTablesWithRetry(dbi);
        dataDir = Files.createTempDir();

        RaptorConnectorId connectorId = new RaptorConnectorId("test");
        NodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        ShardManager shardManager = createShardManager(dbi);
        StorageManager storageManager = createOrcStorageManager(dbi, dataDir);
        StorageManagerConfig config = new StorageManagerConfig();
        connector = new RaptorConnector(
                new LifeCycleManager(ImmutableList.of(), null),
                new TestingNodeManager(),
                new RaptorMetadataFactory(connectorId, dbi, shardManager, functionAndTypeManager),
                new RaptorSplitManager(connectorId, nodeSupplier, shardManager, false),
                new RaptorPageSourceProvider(storageManager),
                new RaptorPageSinkProvider(storageManager,
                        new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                        new TemporalFunction(DateTimeZone.forID("America/Los_Angeles")),
                        config),
                new RaptorNodePartitioningProvider(nodeSupplier),
                new RaptorSessionProperties(config),
                new RaptorTableProperties(functionAndTypeManager),
                ImmutableSet.of(),
                new AllowAllAccessControl(),
                dbi,
                ImmutableSet.of());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        dummyHandle.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
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
    {
        long tableId = createTable("test");

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId));
        metadataDao.blockMaintenance(tableId);
        assertTrue(metadataDao.isMaintenanceBlockedLocked(tableId));

        connector.start();

        assertFalse(metadataDao.isMaintenanceBlockedLocked(tableId));
    }

    @Test
    public void testTemporalShardSplit()
            throws Exception
    {
        // Same date should be in same split
        assertSplitShard(DATE, "2001-08-22", "2001-08-22", "UTC", 1);

        // Date should not be affected by timezone
        assertSplitShard(DATE, "2001-08-22", "2001-08-22", "America/Los_Angeles", 1);

        // User timezone is UTC, while system shard split timezone is PST
        assertSplitShard(TIMESTAMP, "2001-08-22 00:00:01.000", "2001-08-22 23:59:01.000", "UTC", 2);

        // User timezone is PST, while system shard split timezone is PST
        assertSplitShard(TIMESTAMP, "2001-08-22 00:00:01.000", "2001-08-22 23:59:01.000", "America/Los_Angeles", 1);
    }

    private void assertSplitShard(Type temporalType, String min, String max, String userTimeZone, int expectedSplits)
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(
                "user",
                Optional.of("test"),
                Optional.empty(),
                getTimeZoneKey(userTimeZone),
                ENGLISH,
                System.currentTimeMillis(),
                new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties(),
                ImmutableMap.of(),
                true,
                Optional.empty(),
                Optional.empty());

        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        connector.getMetadata(transaction).createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("test", "test"),
                        ImmutableList.of(new ColumnMetadata("id", BIGINT), new ColumnMetadata("time", temporalType)),
                        ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "time", TABLE_SUPPORTS_DELTA_DELETE, false)),
                false);
        connector.commit(transaction);

        ConnectorTransactionHandle txn1 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle1 = getTableHandle(connector.getMetadata(txn1), "test");
        ConnectorInsertTableHandle insertTableHandle = connector.getMetadata(txn1).beginInsert(session, handle1);
        ConnectorPageSink raptorPageSink = connector.getPageSinkProvider().createPageSink(txn1, session, insertTableHandle, PageSinkContext.defaultContext());

        Object timestamp1 = null;
        Object timestamp2 = null;
        if (temporalType.equals(TIMESTAMP)) {
            timestamp1 = new SqlTimestamp(parseTimestampLiteral(getTimeZoneKey(userTimeZone), min), getTimeZoneKey(userTimeZone));
            timestamp2 = new SqlTimestamp(parseTimestampLiteral(getTimeZoneKey(userTimeZone), max), getTimeZoneKey(userTimeZone));
        }
        else if (temporalType.equals(DATE)) {
            timestamp1 = new SqlDate(parseDate(min));
            timestamp2 = new SqlDate(parseDate(max));
        }

        Page inputPage = MaterializedResult.resultBuilder(session, ImmutableList.of(BIGINT, temporalType))
                .row(1L, timestamp1)
                .row(2L, timestamp2)
                .build()
                .toPage();

        raptorPageSink.appendPage(inputPage);

        Collection<Slice> shards = raptorPageSink.finish().get();
        assertEquals(shards.size(), expectedSplits);
        connector.getMetadata(txn1).dropTable(session, handle1);
        connector.commit(txn1);
    }

    private long createTable(String name)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        connector.getMetadata(transaction).createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("test", name),
                        ImmutableList.of(new ColumnMetadata("id", BIGINT)),
                        ImmutableMap.of(TABLE_SUPPORTS_DELTA_DELETE, false)),
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
