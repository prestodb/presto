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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.TransactionManager;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.google.common.collect.ImmutableSet;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalLong;

import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.TestingSchema.createTestingSchema;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCommitCleaner
{
    @Test
    public void testCleaner()
    {
        Database database = new TestingDatabase();
        new SchemaCreator(database).create();

        TestingEnvironment environment = new TestingEnvironment(database);
        Metadata metadata = environment.getMetadata();

        TransactionManager transactionManager = new TransactionManager(metadata);
        CommitCleaner cleaner = new CommitCleaner(transactionManager, database, new CommitCleanerConfig());

        createTestingSchema(environment);

        // verify
        MasterTestingDao master = createJdbi(database.getMasterConnection()).onDemand(MasterTestingDao.class);
        assertThat(master.commitIds()).containsExactly(1L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).containsExactly(1L, 1L);
        assertThat(master.columnCount()).isEqualTo(5);
        assertThat(master.viewIds()).containsExactly(1L);
        assertThat(master.deletedChunkIds()).isEmpty();
        for (Database.Shard shard : database.getShards()) {
            ShardTestingDao dao = createJdbi(shard.getConnection()).onDemand(ShardTestingDao.class);
            assertThat(dao.chunkIds()).containsExactly(1L, 2L, 3L);
            assertThat(dao.indexChunkIds(1L)).containsExactly(1L, 2L, 3L);
        }

        // clean commits
        cleaner.coordinatorRemoveOldCommits();

        // verify the extra table row is gone
        assertThat(master.commitIds()).containsExactly(1L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).containsExactly(1L);
        assertThat(master.columnCount()).isEqualTo(5);
        assertThat(master.viewIds()).containsExactly(1L);
        assertThat(master.deletedChunkIds()).isEmpty();

        // drop view
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        transaction.dropView(1L);
        environment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());

        // verify sizes
        assertThat(master.commitIds()).containsExactly(1L, 2L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).containsExactly(1L);
        assertThat(master.columnCount()).isEqualTo(5);
        assertThat(master.viewIds()).containsExactly(1L);
        assertThat(master.deletedChunkIds()).isEmpty();

        // clean commits
        cleaner.coordinatorRemoveOldCommits();

        // verify sizes
        assertThat(master.commitIds()).containsExactly(2L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).containsExactly(1L);
        assertThat(master.columnCount()).isEqualTo(5);
        assertThat(master.viewIds()).isEmpty();
        assertThat(master.deletedChunkIds()).isEmpty();

        // delete single chunk
        transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        transaction.deleteChunks(1L, ImmutableSet.of(2L));
        environment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());

        // verify sizes
        assertThat(master.commitIds()).containsExactly(2L, 3L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).containsExactly(1L, 1L);
        assertThat(master.columnCount()).isEqualTo(5);
        assertThat(master.viewIds()).isEmpty();
        assertThat(master.deletedChunkIds()).isEmpty();

        // clean commits
        cleaner.coordinatorRemoveOldCommits();

        // verify chunk is deleted
        assertThat(master.commitIds()).containsExactly(3L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).containsExactly(1L);
        assertThat(master.columnCount()).isEqualTo(5);
        assertThat(master.viewIds()).isEmpty();
        assertThat(master.deletedChunkIds()).containsExactly(2L);
        for (Database.Shard shard : database.getShards()) {
            ShardTestingDao dao = createJdbi(shard.getConnection()).onDemand(ShardTestingDao.class);
            assertThat(dao.chunkIds()).containsExactly(1L, 3L);
            assertThat(dao.indexChunkIds(1L)).containsExactly(1L, 3L);
        }

        // drop table
        transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        transaction.dropTable(1L);
        environment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());
        cleaner.coordinatorRemoveOldCommits();

        // verify tables, chunks and index tables are dropped
        assertThat(master.commitIds()).containsExactly(4L);
        assertThat(master.schemaIds()).containsExactly(1L);
        assertThat(master.tableIds()).isEmpty();
        assertThat(master.columnCount()).isEqualTo(0);
        assertThat(master.viewIds()).isEmpty();
        assertThat(master.deletedChunkIds()).containsExactly(1L, 2L, 3L);
        for (Database.Shard shard : database.getShards()) {
            ShardTestingDao dao = createJdbi(shard.getConnection()).onDemand(ShardTestingDao.class);
            assertThat(dao.chunkIds()).isEmpty();
            assertThat(dao.tableNames()).doesNotContain(chunkIndexTable(1L));
        }

        // drop schema
        transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        transaction.dropSchema("tpch");
        environment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());
        cleaner.coordinatorRemoveOldCommits();

        // verify sizes
        assertThat(master.commitIds()).containsExactly(5L);
        assertThat(master.schemaIds()).isEmpty();
        assertThat(master.tableIds()).isEmpty();
        assertThat(master.columnCount()).isEqualTo(0);
        assertThat(master.viewIds()).isEmpty();
        assertThat(master.deletedChunkIds()).containsExactly(1L, 2L, 3L);
    }

    public interface MasterTestingDao
    {
        @SqlQuery("SELECT commit_id FROM commits ORDER BY commit_id")
        List<Long> commitIds();

        @SqlQuery("SELECT schema_id FROM schemata ORDER BY schema_id")
        List<Long> schemaIds();

        @SqlQuery("SELECT table_id FROM tables ORDER BY table_id")
        List<Long> tableIds();

        @SqlQuery("SELECT count(*) FROM columns")
        long columnCount();

        @SqlQuery("SELECT view_id FROM views ORDER BY view_id")
        List<Long> viewIds();

        @SqlQuery("SELECT chunk_id FROM deleted_chunks ORDER BY chunk_id")
        List<Long> deletedChunkIds();
    }

    public interface ShardTestingDao
    {
        @SqlQuery("SELECT chunk_id FROM chunks ORDER BY chunk_id")
        List<Long> chunkIds();

        @SqlQuery("SELECT chunk_id FROM <table> ORDER BY chunk_id")
        List<Long> indexChunkIds(@Define String table);

        default List<Long> indexChunkIds(long tableId)
        {
            return indexChunkIds(chunkIndexTable(tableId));
        }

        @SqlQuery("SELECT table_name FROM information_schema.tables")
        List<String> tableNames();
    }
}
