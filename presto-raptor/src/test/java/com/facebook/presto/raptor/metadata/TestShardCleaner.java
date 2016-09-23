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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.backup.FileBackupStore;
import com.facebook.presto.raptor.storage.FileStorageService;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.raptor.util.UuidUtil.UuidArgumentFactory;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestShardCleaner
{
    private IDBI dbi;
    private Handle dummyHandle;
    private File temporary;
    private StorageService storageService;
    private BackupStore backupStore;
    private TestingTicker ticker;
    private ShardCleaner cleaner;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);

        temporary = createTempDir();
        File directory = new File(temporary, "data");
        storageService = new FileStorageService(directory);
        storageService.start();

        File backupDirectory = new File(temporary, "backup");
        backupStore = new FileBackupStore(backupDirectory);
        ((FileBackupStore) backupStore).start();

        ticker = new TestingTicker();

        ShardCleanerConfig config = new ShardCleanerConfig();
        cleaner = new ShardCleaner(
                new DaoSupplier<>(dbi, H2ShardDao.class),
                "node1",
                true,
                ticker,
                storageService,
                Optional.of(backupStore),
                config.getMaxTransactionAge(),
                config.getTransactionCleanerInterval(),
                config.getLocalCleanerInterval(),
                config.getLocalCleanTime(),
                config.getBackupCleanerInterval(),
                config.getBackupCleanTime(),
                config.getBackupDeletionThreads(),
                config.getMaxCompletedTransactionAge());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        deleteRecursively(temporary);
    }

    @Test
    public void testAbortOldTransactions()
            throws Exception
    {
        TestingDao dao = dbi.onDemand(TestingDao.class);

        long now = System.currentTimeMillis();

        long txn1 = dao.insertTransaction(new Timestamp(now - HOURS.toMillis(26)));
        long txn2 = dao.insertTransaction(new Timestamp(now - HOURS.toMillis(25)));
        long txn3 = dao.insertTransaction(new Timestamp(now));

        ShardDao shardDao = dbi.onDemand(ShardDao.class);
        assertEquals(shardDao.finalizeTransaction(txn1, true), 1);

        assertQuery("SELECT transaction_id, successful FROM transactions",
                row(txn1, true),
                row(txn2, null),
                row(txn3, null));

        cleaner.abortOldTransactions();

        assertQuery("SELECT transaction_id, successful FROM transactions",
                row(txn1, true),
                row(txn2, false),
                row(txn3, null));
    }

    @Test
    public void testDeleteOldShards()
            throws Exception
    {
        assertEquals(cleaner.getBackupShardsQueued().getTotalCount(), 0);

        ShardDao dao = dbi.onDemand(ShardDao.class);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();

        // shards for failed transaction
        long txn1 = dao.insertTransaction();
        assertEquals(dao.finalizeTransaction(txn1, false), 1);

        dao.insertCreatedShard(shard1, txn1);
        dao.insertCreatedShard(shard2, txn1);

        // shards for running transaction
        long txn2 = dao.insertTransaction();

        dao.insertCreatedShard(shard3, txn2);

        // verify database
        assertQuery("SELECT shard_uuid, transaction_id FROM created_shards",
                row(shard1, txn1),
                row(shard2, txn1),
                row(shard3, txn2));

        assertQuery("SELECT shard_uuid FROM deleted_shards");

        // move shards for failed transaction to deleted
        cleaner.deleteOldShards();

        assertEquals(cleaner.getBackupShardsQueued().getTotalCount(), 2);

        // verify database
        assertQuery("SELECT shard_uuid, transaction_id FROM created_shards",
                row(shard3, txn2));

        assertQuery("SELECT shard_uuid FROM deleted_shards",
                row(shard1),
                row(shard2));
    }

    @Test
    public void testCleanLocalShards()
            throws Exception
    {
        assertEquals(cleaner.getLocalShardsCleaned().getTotalCount(), 0);

        TestingShardDao shardDao = dbi.onDemand(TestingShardDao.class);
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        long tableId = metadataDao.insertTable("test", "test", false, false, null, 0);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();
        UUID shard4 = randomUUID();

        Set<UUID> shards = ImmutableSet.of(shard1, shard2, shard3, shard4);

        for (UUID shard : shards) {
            shardDao.insertShard(shard, tableId, null, 0, 0, 0);
            createShardFile(shard);
            assertTrue(shardFileExists(shard));
        }

        int node1 = shardDao.insertNode("node1");
        int node2 = shardDao.insertNode("node2");

        // shard 1: referenced by this node
        // shard 2: not referenced
        // shard 3: not referenced
        // shard 4: referenced by other node

        shardDao.insertShardNode(shard1, node1);
        shardDao.insertShardNode(shard4, node2);

        // mark unreferenced shards
        cleaner.cleanLocalShards();

        assertEquals(cleaner.getLocalShardsCleaned().getTotalCount(), 0);

        // make sure nothing is deleted
        for (UUID shard : shards) {
            assertTrue(shardFileExists(shard));
        }

        // add reference for shard 3
        shardDao.insertShardNode(shard3, node1);

        // advance time beyond clean time
        Duration cleanTime = new ShardCleanerConfig().getLocalCleanTime();
        ticker.increment(cleanTime.toMillis() + 1, MILLISECONDS);

        // clean shards
        cleaner.cleanLocalShards();

        assertEquals(cleaner.getLocalShardsCleaned().getTotalCount(), 2);

        // shards 2 and 4 should be deleted
        // shards 1 and 3 are referenced by this node
        assertTrue(shardFileExists(shard1));
        assertFalse(shardFileExists(shard2));
        assertTrue(shardFileExists(shard3));
        assertFalse(shardFileExists(shard4));
    }

    @Test
    public void testCleanBackupShards()
            throws Exception
    {
        assertEquals(cleaner.getBackupShardsCleaned().getTotalCount(), 0);

        TestingDao dao = dbi.onDemand(TestingDao.class);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();

        long now = System.currentTimeMillis();
        Timestamp time1 = new Timestamp(now - HOURS.toMillis(25));
        Timestamp time2 = new Timestamp(now - HOURS.toMillis(23));

        // shard 1: should be cleaned
        dao.insertDeletedShard(shard1, time1);

        // shard 2: should be cleaned
        dao.insertDeletedShard(shard2, time1);

        // shard 3: deleted too recently
        dao.insertDeletedShard(shard3, time2);

        createShardBackups(shard1, shard2, shard3);

        cleaner.cleanBackupShards();

        assertEquals(cleaner.getBackupShardsCleaned().getTotalCount(), 2);

        assertFalse(shardBackupExists(shard1));
        assertFalse(shardBackupExists(shard2));
        assertTrue(shardBackupExists(shard3));

        assertQuery("SELECT shard_uuid FROM deleted_shards",
                row(shard3));
    }

    @Test
    public void testDeleteOldCompletedTransactions()
            throws Exception
    {
        TestingDao dao = dbi.onDemand(TestingDao.class);
        ShardDao shardDao = dbi.onDemand(ShardDao.class);

        long now = System.currentTimeMillis();
        Timestamp yesterdayStart = new Timestamp(now - HOURS.toMillis(27));
        Timestamp yesterdayEnd = new Timestamp(now - HOURS.toMillis(26));
        Timestamp todayEnd = new Timestamp(now - HOURS.toMillis(1));

        long txn1 = dao.insertTransaction(yesterdayStart);
        long txn2 = dao.insertTransaction(yesterdayStart);
        long txn3 = dao.insertTransaction(yesterdayStart);
        long txn4 = dao.insertTransaction(yesterdayStart);
        long txn5 = dao.insertTransaction(new Timestamp(now));
        long txn6 = dao.insertTransaction(new Timestamp(now));

        assertEquals(shardDao.finalizeTransaction(txn1, true), 1);
        assertEquals(shardDao.finalizeTransaction(txn2, false), 1);
        assertEquals(shardDao.finalizeTransaction(txn3, false), 1);
        assertEquals(shardDao.finalizeTransaction(txn5, true), 1);
        assertEquals(shardDao.finalizeTransaction(txn6, false), 1);

        assertEquals(dao.updateTransactionEndTime(txn1, yesterdayEnd), 1);
        assertEquals(dao.updateTransactionEndTime(txn2, yesterdayEnd), 1);
        assertEquals(dao.updateTransactionEndTime(txn3, yesterdayEnd), 1);
        assertEquals(dao.updateTransactionEndTime(txn5, todayEnd), 1);
        assertEquals(dao.updateTransactionEndTime(txn6, todayEnd), 1);

        shardDao.insertCreatedShard(randomUUID(), txn2);
        shardDao.insertCreatedShard(randomUUID(), txn2);

        assertQuery("SELECT transaction_id, successful, end_time FROM transactions",
                row(txn1, true, yesterdayEnd),  // old successful
                row(txn2, false, yesterdayEnd), // old failed, shards present
                row(txn3, false, yesterdayEnd), // old failed, no referencing shards
                row(txn4, null, null),          // old not finished
                row(txn5, true, todayEnd),      // new successful
                row(txn6, false, todayEnd));    // new failed, no referencing shards

        cleaner.deleteOldCompletedTransactions();

        assertQuery("SELECT transaction_id, successful, end_time FROM transactions",
                row(txn2, false, yesterdayEnd),
                row(txn4, null, null),
                row(txn5, true, todayEnd),
                row(txn6, false, todayEnd));
    }

    private boolean shardFileExists(UUID uuid)
    {
        return storageService.getStorageFile(uuid).exists();
    }

    private void createShardFile(UUID uuid)
            throws IOException
    {
        File file = storageService.getStorageFile(uuid);
        storageService.createParents(file);
        assertTrue(file.createNewFile());
    }

    private boolean shardBackupExists(UUID uuid)
    {
        return backupStore.shardExists(uuid);
    }

    private void createShardBackups(UUID... uuids)
            throws IOException
    {
        for (UUID uuid : uuids) {
            File file = new File(temporary, "empty-" + randomUUID());
            assertTrue(file.createNewFile());
            backupStore.backupShard(uuid, file);
        }
    }

    @SafeVarargs
    private final void assertQuery(@Language("SQL") String sql, List<Object>... rows)
            throws SQLException
    {
        assertEqualsIgnoreOrder(select(sql), asList(rows));
    }

    private List<List<Object>> select(@Language("SQL") String sql)
            throws SQLException
    {
        return dbi.withHandle(handle -> handle.createQuery(sql)
                .map((index, rs, context) -> {
                    int count = rs.getMetaData().getColumnCount();
                    List<Object> row = new ArrayList<>(count);
                    for (int i = 1; i <= count; i++) {
                        Object value = rs.getObject(i);
                        if (value instanceof byte[]) {
                            value = uuidFromBytes((byte[]) value);
                        }
                        row.add(value);
                    }
                    return row;
                })
                .list());
    }

    private static List<Object> row(Object... values)
    {
        return asList(values);
    }

    @RegisterArgumentFactory(UuidArgumentFactory.class)
    private interface TestingDao
    {
        @SqlUpdate("INSERT INTO transactions (start_time) VALUES (:startTime)")
        @GetGeneratedKeys
        long insertTransaction(@Bind("startTime") Timestamp timestamp);

        @SqlUpdate("INSERT INTO deleted_shards (shard_uuid, delete_time)\n" +
                "VALUES (:shardUuid, :deleteTime)")
        void insertDeletedShard(
                @Bind("shardUuid") UUID shardUuid,
                @Bind("deleteTime") Timestamp deleteTime);

        @SqlUpdate("UPDATE transactions SET end_time = :endTime WHERE transaction_id = :transactionId")
        int updateTransactionEndTime(@Bind("transactionId") long transactionId, @Bind("endTime") Timestamp endTime);
    }
}
