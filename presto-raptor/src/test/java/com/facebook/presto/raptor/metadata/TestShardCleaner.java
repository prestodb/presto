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
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
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

        ShardCleanerConfig config = new ShardCleanerConfig();
        cleaner = new ShardCleaner(
                new DaoSupplier<>(dbi, ShardDao.class),
                "node1",
                true,
                storageService,
                Optional.of(backupStore),
                config.getMaxTransactionAge(),
                config.getTransactionCleanerInterval(),
                config.getLocalCleanerInterval(),
                config.getLocalCleanTime(),
                config.getLocalPurgeTime(),
                config.getBackupCleanerInterval(),
                config.getBackupCleanTime(),
                config.getBackupPurgeTime(),
                config.getBackupDeletionThreads());
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
        ShardDao dao = dbi.onDemand(ShardDao.class);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();

        int node1 = dao.insertNode("node1");
        int node2 = dao.insertNode("node2");

        // shards for failed transaction
        long txn1 = dao.insertTransaction();
        assertEquals(dao.finalizeTransaction(txn1, false), 1);

        dao.insertCreatedShard(shard1, txn1);
        dao.insertCreatedShardNode(shard1, node1, txn1);

        dao.insertCreatedShard(shard2, txn1);
        dao.insertCreatedShardNode(shard2, node2, txn1);

        // shards for running transaction
        long txn2 = dao.insertTransaction();

        dao.insertCreatedShard(shard3, txn2);
        dao.insertCreatedShardNode(shard3, node1, txn2);

        // verify database
        assertQuery("SELECT shard_uuid, transaction_id FROM created_shards",
                row(shard1, txn1),
                row(shard2, txn1),
                row(shard3, txn2));

        assertQuery("SELECT shard_uuid, node_id, transaction_id FROM created_shard_nodes",
                row(shard1, node1, txn1),
                row(shard2, node2, txn1),
                row(shard3, node1, txn2));

        assertQuery("SELECT shard_uuid FROM deleted_shards");
        assertQuery("SELECT shard_uuid, node_id FROM deleted_shard_nodes");

        // move shards for failed transaction to deleted
        cleaner.deleteOldShards();

        // verify database
        assertQuery("SELECT shard_uuid, transaction_id FROM created_shards",
                row(shard3, txn2));

        assertQuery("SELECT shard_uuid, node_id, transaction_id FROM created_shard_nodes",
                row(shard3, node1, txn2));

        assertQuery("SELECT shard_uuid FROM deleted_shards",
                row(shard1),
                row(shard2));

        assertQuery("SELECT shard_uuid, node_id FROM deleted_shard_nodes",
                row(shard1, node1),
                row(shard2, node2));
    }

    @Test
    public void testCleanLocalShards()
            throws Exception
    {
        TestingDao dao = dbi.onDemand(TestingDao.class);
        ShardDao shardDao = dbi.onDemand(ShardDao.class);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();
        UUID shard4 = randomUUID();

        int node1 = shardDao.insertNode("node1");
        int node2 = shardDao.insertNode("node2");

        long now = System.currentTimeMillis();
        Timestamp time1 = new Timestamp(now - HOURS.toMillis(5));
        Timestamp time2 = new Timestamp(now - HOURS.toMillis(3));

        // shard 1: should be cleaned
        dao.insertDeletedShardNode(shard1, node1, time1);

        // shard 2: should be cleaned
        dao.insertDeletedShardNode(shard2, node1, time1);

        // shard 3: deleted too recently
        dao.insertDeletedShardNode(shard3, node1, time2);

        // shard 4: on different node
        dao.insertDeletedShardNode(shard4, node2, time1);

        createShardFiles(shard1, shard2, shard3, shard4);

        cleaner.cleanLocalShards();

        assertFalse(shardFileExists(shard1));
        assertFalse(shardFileExists(shard2));
        assertTrue(shardFileExists(shard3));
        assertTrue(shardFileExists(shard4));

        assertQuery("SELECT shard_uuid, node_id, clean_time IS NULL FROM deleted_shard_nodes",
                row(shard1, node1, false),
                row(shard2, node1, false),
                row(shard3, node1, true),
                row(shard4, node2, true));
    }

    @Test
    public void testPurgeLocalShards()
            throws Exception
    {
        TestingDao dao = dbi.onDemand(TestingDao.class);
        ShardDao shardDao = dbi.onDemand(ShardDao.class);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();
        UUID shard4 = randomUUID();

        int node1 = shardDao.insertNode("node1");
        int node2 = shardDao.insertNode("node2");

        long now = System.currentTimeMillis();
        Timestamp time1 = new Timestamp(now - DAYS.toMillis(4));
        Timestamp time2 = new Timestamp(now - DAYS.toMillis(2));

        // shard 1: should be purged
        dao.insertCleanedDeletedShardNode(shard1, node1, time1);

        // shard 2: should be purged
        dao.insertCleanedDeletedShardNode(shard2, node1, time1);

        // shard 3: cleaned too recently
        dao.insertCleanedDeletedShardNode(shard3, node1, time2);

        // shard 4: on different node
        dao.insertDeletedShardNode(shard4, node2, time1);

        createShardFiles(shard1, shard2, shard3, shard4);

        cleaner.purgeLocalShards();

        assertFalse(shardFileExists(shard1));
        assertFalse(shardFileExists(shard2));
        assertTrue(shardFileExists(shard3));
        assertTrue(shardFileExists(shard4));

        assertQuery("SELECT shard_uuid, node_id FROM deleted_shard_nodes",
                row(shard3, node1),
                row(shard4, node2));
    }

    @Test
    public void testCleanBackupShards()
            throws Exception
    {
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

        assertFalse(shardBackupExists(shard1));
        assertFalse(shardBackupExists(shard2));
        assertTrue(shardBackupExists(shard3));

        assertQuery("SELECT shard_uuid, clean_time IS NULL FROM deleted_shards",
                row(shard1, false),
                row(shard2, false),
                row(shard3, true));
    }

    @Test
    public void testPurgeBackupShards()
            throws Exception
    {
        TestingDao dao = dbi.onDemand(TestingDao.class);

        UUID shard1 = randomUUID();
        UUID shard2 = randomUUID();
        UUID shard3 = randomUUID();

        long now = System.currentTimeMillis();
        Timestamp time1 = new Timestamp(now - DAYS.toMillis(4));
        Timestamp time2 = new Timestamp(now - DAYS.toMillis(2));

        // shard 1: should be purged
        dao.insertCleanedDeletedShard(shard1, time1);

        // shard 2: should be purged
        dao.insertCleanedDeletedShard(shard2, time1);

        // shard 3: cleaned too recently
        dao.insertCleanedDeletedShard(shard3, time2);

        createShardBackups(shard1, shard2, shard3);

        cleaner.purgeBackupShards();

        assertFalse(shardBackupExists(shard1));
        assertFalse(shardBackupExists(shard2));
        assertTrue(shardBackupExists(shard3));

        assertQuery("SELECT shard_uuid FROM deleted_shards",
                row(shard3));
    }

    private boolean shardFileExists(UUID uuid)
    {
        return storageService.getStorageFile(uuid).exists();
    }

    private void createShardFiles(UUID... uuids)
            throws IOException
    {
        for (UUID uuid : uuids) {
            File file = storageService.getStorageFile(uuid);
            storageService.createParents(file);
            assertTrue(file.createNewFile());
        }
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

        @SqlUpdate("INSERT INTO deleted_shard_nodes (shard_uuid, node_id, delete_time)\n" +
                "VALUES (:shardUuid, :nodeId, :deleteTime)")
        void insertDeletedShardNode(
                @Bind("shardUuid") UUID shardUuid,
                @Bind("nodeId") int nodeId,
                @Bind("deleteTime") Timestamp deleteTime);

        @SqlUpdate("INSERT INTO deleted_shards (shard_uuid, delete_time, clean_time)\n" +
                "VALUES (:shardUuid, :cleanTime, :cleanTime)")
        void insertCleanedDeletedShard(
                @Bind("shardUuid") UUID shardUuid,
                @Bind("cleanTime") Timestamp cleanTime);

        @SqlUpdate("INSERT INTO deleted_shard_nodes (shard_uuid, node_id, delete_time, clean_time)\n" +
                "VALUES (:shardUuid, :nodeId, :cleanTime, :cleanTime)")
        void insertCleanedDeletedShardNode(
                @Bind("shardUuid") UUID shardUuid,
                @Bind("nodeId") int nodeId,
                @Bind("cleanTime") Timestamp cleanTime);
    }
}
