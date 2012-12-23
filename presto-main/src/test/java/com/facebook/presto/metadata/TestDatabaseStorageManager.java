package com.facebook.presto.metadata;

import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordSet;
import com.facebook.presto.ingest.InMemoryRecordSet;
import com.facebook.presto.operator.AlignmentOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.operator.OperatorAssertions.assertOperatorEquals;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static io.airlift.units.DataSize.Unit.BYTE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDatabaseStorageManager
{
    private Handle dummyHandle;
    private File dataDir;
    private StorageManager storageManager;

    @BeforeMethod
    public void setupDatabase()
            throws IOException
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        StorageManagerConfig config = new StorageManagerConfig().setDataDirectory(dataDir);
        storageManager = new DatabaseStorageManager(dbi, config);
    }

    @AfterMethod
    public void cleanupDatabase()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testImportFlow()
            throws IOException
    {
        long shardId = 123;
        List<Long> columnIds = ImmutableList.of(7L, 11L);

        RecordSet records = new InMemoryRecordSet(ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc", 1L), ImmutableList.of("def", 2L), ImmutableList.of("g", 0L)}));
        RecordProjectOperator source = new RecordProjectOperator(records, new DataSize(10, BYTE), VARIABLE_BINARY, FIXED_INT_64);

        assertFalse(storageManager.shardExists(shardId));

        storageManager.importShard(shardId, columnIds, source);

        assertTrue(storageManager.shardExists(shardId));

        assertOperatorEquals(
                new AlignmentOperator(storageManager.getBlocks(shardId, columnIds.get(0)), storageManager.getBlocks(shardId, columnIds.get(1))),
                new RecordProjectOperator(records, new DataSize(10, BYTE), VARIABLE_BINARY, FIXED_INT_64));
    }

    @Test
    public void testImportEmptySource()
            throws IOException
    {
        long shardId = 456;
        List<Long> columnIds = ImmutableList.of(13L);

        RecordSet records = new InMemoryRecordSet(ImmutableList.copyOf(new List<?>[]{}));
        RecordProjectOperator source = new RecordProjectOperator(records, new DataSize(10, BYTE), VARIABLE_BINARY);

        assertFalse(storageManager.shardExists(shardId));

        storageManager.importShard(shardId, columnIds, source);

        assertTrue(storageManager.shardExists(shardId));

        assertTrue(Iterables.isEmpty(storageManager.getBlocks(shardId, columnIds.get(0))));

        // TODO: make this work after empty blocks are supported
//        assertOperatorEquals(
//                new AlignmentOperator(storageManager.getBlocks(shardId, columnIds.get(0))),
//                new RecordProjectOperator(records, createProjection(0, VARIABLE_BINARY)));
    }
}
