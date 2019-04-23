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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.TransactionManager;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.CommitCleaner;
import com.facebook.presto.raptorx.metadata.CommitCleanerConfig;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.ShardCommitCleanerDao;
import com.facebook.presto.raptorx.metadata.ShardWriterDao;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.storage.OrcStorageManager;
import com.facebook.presto.raptorx.storage.ReaderAttributes;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.metadata.ShardHashing.dbShard;
import static com.facebook.presto.raptorx.storage.TestOrcStorageManager.createOrcStorageManager;
import static com.facebook.presto.raptorx.storage.organization.TestChunkCompactor.createChunks;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrganizationJob
{
    private static final int MAX_CHUNK_ROWS = 1000;
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

    private OrcStorageManager storageManager;
    private ChunkCompactor compactor;
    private File temporary;
    private TestingDatabase database;
    private TestingEnvironment testingEnvironment;
    private TransactionManager transactionManager;
    private List<ShardCommitCleanerDao> shardDao;
    private List<ShardWriterDao> shardWriterDao;
    private CommitCleaner commitCleaner;
    private long distributionId;

    private static final ColumnInfo BUCKET_COLUMN = new ColumnInfo(1, "1", BIGINT, Optional.empty(), 1, OptionalInt.of(1), OptionalInt.empty());
    private static final ColumnInfo COLUMN = new ColumnInfo(5, "5", BIGINT, Optional.empty(), 1, OptionalInt.empty(), OptionalInt.empty());
    private static final List<ColumnInfo> COLUMNS = ImmutableList.of(BUCKET_COLUMN, COLUMN);
    private static final TableInfo bucketedTemporalTableInfo = new TableInfo(23L, "3", 10, 1, OptionalLong.of(1L), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), COLUMNS);

    @BeforeMethod
    public void setup()
    {
        temporary = createTempDir();
        database = new TestingDatabase();
        new SchemaCreator(database).create();
        testingEnvironment = new TestingEnvironment(database);
        transactionManager = new TransactionManager(testingEnvironment.getMetadata());
        storageManager = createOrcStorageManager(database, temporary, MAX_CHUNK_ROWS);
        compactor = new ChunkCompactor(storageManager, READER_ATTRIBUTES);
        shardDao = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .map(dbi -> dbi.onDemand(ShardCommitCleanerDao.class))
                .collect(toImmutableList());
        shardWriterDao = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .map(dbi -> dbi.onDemand(ShardWriterDao.class))
                .collect(toImmutableList());
        distributionId = testingEnvironment.getMetadata().createDistribution(Optional.empty(), ImmutableList.of(), nCopies(5, 1L));
        commitCleaner = new CommitCleaner(transactionManager, database, new CommitCleanerConfig());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (database != null) {
            database.close();
        }
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testSuccessOrganization()
            throws Exception
    {
        long tableId = bucketedTemporalTableInfo.getTableId();
        createTable(tableId);
        List<Long> columnIds = ImmutableList.of(1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, BIGINT);
        List<ChunkInfo> inputChunks = createChunks(storageManager, columnIds, columnTypes, 3);
        assertEquals(inputChunks.size(), 3);

        assertEquals(countWorkerTransactions(true, 10L), 0);
        assertEquals(countWorkerTransactions(true, 15L), 0);

        // 1st set
        insertChunks(tableId, inputChunks);

        Set<Long> inputChunkIds = inputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());
        OrganizationSet set = new OrganizationSet(bucketedTemporalTableInfo, inputChunkIds, 1);
        OrganizationJob organizationJob = new OrganizationJob(set, compactor, testingEnvironment.getMetadataWriter(), transactionManager, 10L);
        organizationJob.run();

        // 2nd set
        inputChunks = createChunks(storageManager, columnIds, columnTypes, 2);
        assertEquals(inputChunks.size(), 2);
        insertChunks(tableId, inputChunks);

        inputChunkIds = inputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());
        set = new OrganizationSet(bucketedTemporalTableInfo, inputChunkIds, 2);
        organizationJob = new OrganizationJob(set, compactor, testingEnvironment.getMetadataWriter(), transactionManager, 15L);
        organizationJob.run();

        // worker_transactions should have 2 successful transactions
        assertEquals(countWorkerTransactions(true, 10L), 1);
        assertEquals(countWorkerTransactions(true, 15L), 1);

        // Coordinator shouldn't change any
        commitCleaner.coordinatorRemoveOldCommits();
        assertEquals(countWorkerTransactions(true, 10L), 1);
        assertEquals(countWorkerTransactions(true, 15L), 1);

        // Worker Cleaner should remove successful worker transactions
        commitCleaner.removeOldWorkerTransactions(10L);
        assertEquals(countWorkerTransactions(true, 10L), 0);
        assertEquals(countWorkerTransactions(true, 15L), 1);

        commitCleaner.removeOldWorkerTransactions(15L);
        assertEquals(countWorkerTransactions(true, 10L), 0);
        assertEquals(countWorkerTransactions(true, 15L), 0);
    }

    @Test
    public void testFailOrganization()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, BIGINT);
        List<ChunkInfo> inputChunks = createChunks(storageManager, columnIds, columnTypes, 3);
        assertEquals(inputChunks.size(), 3);

        assertEquals(countWorkerTransactions(false, 10L), 0);

        Set<Long> inputChunkIds = inputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());
        OrganizationSet set = new OrganizationSet(bucketedTemporalTableInfo, inputChunkIds, 1);
        OrganizationJob organizationJob = new OrganizationJob(set, compactor, testingEnvironment.getMetadataWriter(), transactionManager, 10L);

        // chunks not in DB, DeleteChunks op should find conflict and fail
        try {
            organizationJob.run();
        }
        catch (Exception e) {
            // expect Exception
        }
        assertEquals(countWorkerTransactions(false, 10L), 1);
        assertTrue(transactionManager.activeTransactions().isEmpty());

        // nodeId 25 doesn't remove anything of nodeId 10
        commitCleaner.removeOldWorkerTransactions(25L);
        assertEquals(countWorkerTransactions(false, 10L), 1);

        // Coordinator routine also doesn't do anything
        commitCleaner.coordinatorRemoveOldCommits();
        assertEquals(countWorkerTransactions(false, 10L), 1);

        // nodeId 10 should remove everything of nodeId 10
        commitCleaner.removeOldWorkerTransactions(10L);
        assertEquals(countWorkerTransactions(false, 10L), 0);
    }

    @Test
    public void testMaintenance()
            throws Exception
    {
        long tableId = bucketedTemporalTableInfo.getTableId();
        createTable(tableId);
        ShardWriterDao dao = shardWriterDao.get(dbShard(tableId, shardWriterDao.size()));
        dao.blockMaintenance(tableId, System.currentTimeMillis());
        try {
            dao.blockMaintenance(tableId, System.currentTimeMillis());
        }
        catch (Exception e) {
            // expect Exception
        }

        List<Long> columnIds = ImmutableList.of(1L, 5L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, BIGINT);
        List<ChunkInfo> inputChunks = createChunks(storageManager, columnIds, columnTypes, 3);
        assertEquals(inputChunks.size(), 3);

        assertEquals(countWorkerTransactions(true, 10L), 0);
        assertEquals(countWorkerTransactions(true, 15L), 0);

        // 1st set
        insertChunks(tableId, inputChunks);

        Set<Long> inputChunkIds = inputChunks.stream().map(ChunkInfo::getChunkId).collect(toSet());
        OrganizationSet set = new OrganizationSet(bucketedTemporalTableInfo, inputChunkIds, 1);
        OrganizationJob organizationJob = new OrganizationJob(set, compactor, testingEnvironment.getMetadataWriter(), transactionManager, 10L);

        try {
            organizationJob.run();
        }
        catch (Exception e) {
            // expect Exception
            e.printStackTrace();
        }

        dao.unBlockMaintenance(tableId);
        organizationJob.run();  // no Exception
    }

    private void createTable(long tableId)
    {
        ConnectorTransactionHandle tHandle = transactionManager.create();
        Transaction transaction = transactionManager.get(tHandle);
        transaction.createTable(tableId, 1, "a", distributionId, OptionalLong.empty(), CompressionType.ZSTD, System.currentTimeMillis(), Optional.empty(), COLUMNS, true);
        testingEnvironment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());
    }

    private void insertChunks(long tableId, Collection<ChunkInfo> chunks)
    {
        ConnectorTransactionHandle tHandle = transactionManager.create();
        Transaction transaction = transactionManager.get(tHandle);
        transaction.insertChunks(tableId, chunks);
        testingEnvironment.getTransactionWriter().write(transaction.getActions(), OptionalLong.empty());
    }

    private int countWorkerTransactions(boolean success, long nodeId)
    {
        int ret = 0;
        for (ShardCommitCleanerDao dao : shardDao) {
            if (success) {
                ret += dao.getSuccessfulTransactionIds(nodeId).size();
            }
            else {
                ret += dao.getFailedTransactions(nodeId).size();
            }
        }
        return ret;
    }
}
