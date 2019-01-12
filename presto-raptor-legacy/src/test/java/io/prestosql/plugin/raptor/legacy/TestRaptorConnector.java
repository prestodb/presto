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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.slice.Slice;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.operator.PagesIndex;
import io.prestosql.plugin.base.security.AllowAllAccessControl;
import io.prestosql.plugin.raptor.legacy.metadata.MetadataDao;
import io.prestosql.plugin.raptor.legacy.metadata.ShardManager;
import io.prestosql.plugin.raptor.legacy.metadata.TableColumn;
import io.prestosql.plugin.raptor.legacy.storage.StorageManager;
import io.prestosql.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.prestosql.plugin.raptor.legacy.storage.organization.TemporalFunction;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.testing.TestingNodeManager;
import io.prestosql.type.TypeRegistry;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.raptor.legacy.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static io.prestosql.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.prestosql.plugin.raptor.legacy.metadata.TestDatabaseShardManager.createShardManager;
import static io.prestosql.plugin.raptor.legacy.storage.TestOrcStorageManager.createOrcStorageManager;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.util.DateTimeUtils.parseDate;
import static io.prestosql.util.DateTimeUtils.parseTimestampLiteral;
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
        StorageManagerConfig config = new StorageManagerConfig();
        connector = new RaptorConnector(
                new LifeCycleManager(ImmutableList.of(), null),
                new TestingNodeManager(),
                new RaptorMetadataFactory(connectorId, dbi, shardManager),
                new RaptorSplitManager(connectorId, nodeSupplier, shardManager, false),
                new RaptorPageSourceProvider(storageManager),
                new RaptorPageSinkProvider(storageManager,
                        new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                        new TemporalFunction(DateTimeZone.forID("America/Los_Angeles")),
                        config),
                new RaptorNodePartitioningProvider(nodeSupplier),
                new RaptorSessionProperties(config),
                new RaptorTableProperties(typeRegistry),
                ImmutableSet.of(),
                new AllowAllAccessControl(),
                dbi);
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
                true);

        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_COMMITTED, false);
        connector.getMetadata(transaction).createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("test", "test"),
                        ImmutableList.of(new ColumnMetadata("id", BIGINT), new ColumnMetadata("time", temporalType)),
                        ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "time")),
                false);
        connector.commit(transaction);

        ConnectorTransactionHandle txn1 = connector.beginTransaction(READ_COMMITTED, false);
        ConnectorTableHandle handle1 = getTableHandle(connector.getMetadata(txn1), "test");
        ConnectorInsertTableHandle insertTableHandle = connector.getMetadata(txn1).beginInsert(session, handle1);
        ConnectorPageSink raptorPageSink = connector.getPageSinkProvider().createPageSink(txn1, session, insertTableHandle);

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
                        ImmutableList.of(new ColumnMetadata("id", BIGINT))),
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
