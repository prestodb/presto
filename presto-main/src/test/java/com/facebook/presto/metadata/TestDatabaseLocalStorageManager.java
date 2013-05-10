package com.facebook.presto.metadata;

import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
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
import static org.testng.Assert.assertEquals;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDatabaseLocalStorageManager
{
    private Handle dummyHandle;
    private File dataDir;
    private LocalStorageManager storageManager;

    @BeforeMethod
    public void setupDatabase()
            throws IOException
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        DatabaseLocalStorageManagerConfig config = new DatabaseLocalStorageManagerConfig().setDataDirectory(dataDir);
        storageManager = new DatabaseLocalStorageManager(dbi, config);
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
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>of(new NativeColumnHandle("column_7", 7L), new NativeColumnHandle("column_11", 11L));

        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(STRING, LONG), ImmutableList.copyOf(new List<?>[]{ImmutableList.of("abc", 1L), ImmutableList.of("def", 2L), ImmutableList.of("g", 0L)}));
        RecordProjectOperator source = new RecordProjectOperator(records);

        assertFalse(storageManager.shardExists(shardId));

        storageManager.importShard(shardId, columnHandles, source);

        assertTrue(storageManager.shardExists(shardId));

        assertOperatorEquals(
                new AlignmentOperator(storageManager.getBlocks(shardId, columnHandles.get(0)), storageManager.getBlocks(shardId, columnHandles.get(1))),
                new RecordProjectOperator(records));
    }

    @Test
    public void testImportEmptySource()
            throws IOException
    {
        long shardId = 456;
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>of(new NativeColumnHandle("column_13", 13L));

        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(STRING), ImmutableList.copyOf(new List<?>[]{}));
        RecordProjectOperator source = new RecordProjectOperator(records);

        assertFalse(storageManager.shardExists(shardId));

        storageManager.importShard(shardId, columnHandles, source);

        assertTrue(storageManager.shardExists(shardId));

        assertTrue(Iterables.isEmpty(storageManager.getBlocks(shardId, columnHandles.get(0))));

        AlignmentOperator actual = new AlignmentOperator(storageManager.getBlocks(shardId, columnHandles.get(0)));
        assertFalse(actual.iterator(new OperatorStats()).hasNext());
    }

    @Test
    public void testShardPath()
    {
        File result = DatabaseLocalStorageManager.getShardPath(new File("/"), 0);
        assertEquals(result.getAbsolutePath(), "/00/00/00/00");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 1);
        assertEquals(result.getAbsolutePath(), "/01/00/00/00");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 100);
        assertEquals(result.getAbsolutePath(), "/00/01/00/00");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 10_000);
        assertEquals(result.getAbsolutePath(), "/00/00/01/00");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 1_000_000);
        assertEquals(result.getAbsolutePath(), "/00/00/00/01");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 99_999_999);
        assertEquals(result.getAbsolutePath(), "/99/99/99/99");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 100_000_000);
        assertEquals(result.getAbsolutePath(), "/00/00/00/100");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 12345);
        assertEquals(result.getAbsolutePath(), "/45/23/01/00");
        result = DatabaseLocalStorageManager.getShardPath(new File("/"), 4815162342L);
        assertEquals(result.getAbsolutePath(), "/42/23/16/4815");
    }
}
