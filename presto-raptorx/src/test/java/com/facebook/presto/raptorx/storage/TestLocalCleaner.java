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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.chunkstore.FileChunkStore;
import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DatabaseChunkManager;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.testing.TestingTicker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptorx.storage.organization.TestChunkOrganizerUtil.chunkInfo;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalCleaner
{
    private TestingDatabase database;
    private File temporary;
    private StorageService storageService;
    private FileChunkStore backupStore;
    private TestingTicker ticker;
    private LocalCleaner cleaner;
    private NodeIdCache nodeIdCache;
    private ChunkManager chunkManager;
    private Metadata metadata;
    private TransactionWriter transactionWriter;

    @BeforeMethod
    public void setup()
    {
        temporary = createTempDir();
        database = new TestingDatabase();
        TestingEnvironment environment = new TestingEnvironment(database);
        new SchemaCreator(database).create();
        metadata = environment.getMetadata();
        this.nodeIdCache = environment.getNodeIdCache();
        chunkManager = new DatabaseChunkManager(nodeIdCache, database, environment.getTypeManager());
        transactionWriter = environment.getTransactionWriter();

        storageService = new FileStorageService(new File(temporary, "data"));
        storageService.start();
        backupStore = new FileChunkStore(new File(temporary, "backup"));
        backupStore.start();

        ticker = new TestingTicker();

        LocalCleanerConfig config = new LocalCleanerConfig();
        cleaner = new LocalCleaner("node1",
                ticker,
                chunkManager,
                storageService,
                config.getLocalCleanerInterval(),
                config.getLocalCleanTime(),
                config.getThreads());
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        if (database != null) {
            database.close();
        }
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCleanLocalChunksImmediately()
            throws Exception
    {
        assertEquals(cleaner.getLocalChunksCleaned().getTotalCount(), 0);

        long node1 = nodeIdCache.getNodeId("node1");
        long node2 = nodeIdCache.getNodeId("node2");
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        // bucket 0  1  2
        // nodeid 1  1  2
        long tableId = metadata.nextTableId();
        long distributionId = transaction.createDistribution(ImmutableList.<Type>builder().add(BIGINT).build(),
                ImmutableList.<Long>builder()
                        .add(node1)
                        .add(node1)
                        .add(node2)
                        .build());
        transaction.createTable(tableId, 7, "test", distributionId, OptionalLong.empty(), CompressionType.ZSTD, System.currentTimeMillis(), Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(new ColumnInfo(10, "orderkey", BIGINT, Optional.empty(), 1, OptionalInt.of(1), OptionalInt.of(1)))
                        .build(), false);
        TableInfo tableInfo = transaction.getTableInfo(tableId);
        List<ColumnInfo> tableColumns = tableInfo.getColumns();
        Map<String, ColumnInfo> tableColumnMap = Maps.uniqueIndex(tableColumns, ColumnInfo::getColumnName);
        long orderKey = tableColumnMap.get("orderkey").getColumnId();

        List<ChunkInfo> chunks = ImmutableList.<ChunkInfo>builder()
                .add(chunkInfo(100, 0, ImmutableList.of(new ColumnStats(orderKey, 13L, 14L))))
                .add(chunkInfo(102, 2, ImmutableList.of(new ColumnStats(orderKey, 2L, 11L))))
                .build();

        transaction.insertChunks(tableId, chunks);
        transactionWriter.write(transaction.getActions(), OptionalLong.empty());

        for (long chunk : ImmutableList.of(100, 101, 102)) {
            createChunkFile(chunk);
            assertTrue(chunkFileExists(chunk));
        }

        Set<Long> local = cleaner.getLocalChunks();
        cleaner.cleanLocalChunksImmediately(local);
        assertEquals(cleaner.getLocalChunksCleaned().getTotalCount(), 2);

        // chunk 101, not in db; chunk 102, bucket2, in node2; should be deleted
        // chunk 100  is referenced by this node1
        assertTrue(chunkFileExists(100));
        assertFalse(chunkFileExists(101));
        assertFalse(chunkFileExists(102));
    }

    private boolean chunkFileExists(long chunkId)
    {
        return storageService.getStorageFile(chunkId).exists();
    }

    private void createChunkFile(long chunkId)
            throws IOException
    {
        File file = storageService.getStorageFile(chunkId);
        assertTrue(file.createNewFile());
    }
}
