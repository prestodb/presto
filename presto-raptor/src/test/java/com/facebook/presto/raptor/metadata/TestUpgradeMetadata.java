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

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToCreateStatementException;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Test the correctness of metadata after upgrading
 * from environment without delta delete to environment with delta delete
 */
@Test(singleThreaded = true)
public class TestUpgradeMetadata
{
    private IDBI dbi;
    private MetadataDao dao;
    private Handle dummyHandle;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
        dummyHandle = dbi.open();
        dao = dbi.onDemand(MetadataDao.class);
        createLegacyTables(dummyHandle.attach(SchemaDao.class), dummyHandle.attach(LegacySchemaDao.class));
    }

    private void createLegacyTables(SchemaDao dao, LegacySchemaDao legacySchemaDao)
    {
        dao.createTableDistributions();
        legacySchemaDao.createTableTablesForTest();
        dao.createTableColumns();
        dao.createTableViews();
        dao.createTableNodes();
        legacySchemaDao.createTableShardsForTest();
        dao.createTableShardNodes();
        dao.createTableExternalBatches();
        dao.createTableTransactions();
        dao.createTableCreatedShards();
        dao.createTableDeletedShards();
        dao.createTableBuckets();
        dao.createTableShardOrganizerJobs();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        dummyHandle.close();
    }

    @Test(expectedExceptions = UnableToCreateStatementException.class)
    public void testTableTablesWithoutUpgrading()
    {
        // without perform upgrading operation would cause metadata incorrect
        dao.insertTable("schema1", "table1", true, false, null, 0, false);
    }

    @Test
    public void testTableTables()
    {
        // perform upgrading operation on metadata
        createTablesWithRetry(dbi);
        long tableId = dao.insertTable("schema1", "table1", true, false, null, 0, false);
        assertGreaterThan(tableId, 0L);
    }

    @Test
    public void testTableShards()
    {
        // perform upgrading operation on metadata
        createTablesWithRetry(dbi);
        TestingShardDao testingShardDao = dbi.onDemand(TestingShardDao.class);
        long tableId = createTable("test");
        UUID shardUuid = UUID.randomUUID();
        UUID deltaUuid = UUID.randomUUID();
        long shardId = testingShardDao.insertShardWithDelta(shardUuid, tableId, false, deltaUuid, null, 13, 42, 84, 1234);

        ShardMetadata shard = testingShardDao.getShard(shardUuid);
        assertNotNull(shard);
        assertEquals(shard.getTableId(), tableId);
        assertEquals(shard.getShardId(), shardId);
        assertEquals(shard.getShardUuid(), shardUuid);
        assertEquals(shard.getRowCount(), 13);
        assertEquals(shard.getCompressedSize(), 42);
        assertEquals(shard.getUncompressedSize(), 84);
        assertEquals(shard.getXxhash64(), OptionalLong.of(1234));
    }

    private long createTable(String name)
    {
        return dbi.onDemand(MetadataDao.class).insertTable("test", name, false, false, null, 0, false);
    }

    /**
     * This class is for generating the schema for environment without delta delete
     */
    interface LegacySchemaDao
    {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
                "  table_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
                "  schema_name VARCHAR(255) NOT NULL,\n" +
                "  table_name VARCHAR(255) NOT NULL,\n" +
                "  temporal_column_id BIGINT,\n" +
                "  compaction_enabled BOOLEAN NOT NULL,\n" +
                "  organization_enabled BOOLEAN NOT NULL,\n" +
                "  distribution_id BIGINT,\n" +
                "  create_time BIGINT NOT NULL,\n" +
                "  update_time BIGINT NOT NULL,\n" +
                "  table_version BIGINT NOT NULL,\n" +
                "  shard_count BIGINT NOT NULL,\n" +
                "  row_count BIGINT NOT NULL,\n" +
                "  compressed_size BIGINT NOT NULL,\n" +
                "  uncompressed_size BIGINT NOT NULL,\n" +
                "  maintenance_blocked DATETIME,\n" +
                "  UNIQUE (schema_name, table_name),\n" +
                "  UNIQUE (distribution_id, table_id),\n" +
                "  UNIQUE (maintenance_blocked, table_id),\n" +
                "  FOREIGN KEY (distribution_id) REFERENCES distributions (distribution_id)\n" +
                ")")
        void createTableTablesForTest();

        @SqlUpdate("CREATE TABLE IF NOT EXISTS shards (\n" +
                "  shard_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
                "  shard_uuid BINARY(16) NOT NULL,\n" +
                "  table_id BIGINT NOT NULL,\n" +
                "  bucket_number INT,\n" +
                "  create_time DATETIME NOT NULL,\n" +
                "  row_count BIGINT NOT NULL,\n" +
                "  compressed_size BIGINT NOT NULL,\n" +
                "  uncompressed_size BIGINT NOT NULL,\n" +
                "  xxhash64 BIGINT NOT NULL,\n" +
                "  UNIQUE (shard_uuid),\n" +
                // include a covering index organized by table_id
                "  UNIQUE (table_id, bucket_number, shard_id, shard_uuid, create_time, row_count, compressed_size, uncompressed_size, xxhash64),\n" +
                "  FOREIGN KEY (table_id) REFERENCES tables (table_id)\n" +
                ")")
        void createTableShardsForTest();
    }
}
