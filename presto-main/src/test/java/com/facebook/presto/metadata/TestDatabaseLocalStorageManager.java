package com.facebook.presto.metadata;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.noperator.DriverContext;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewOperator;
import com.facebook.presto.noperator.NewOperatorAssertion;
import com.facebook.presto.noperator.TaskContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.MaterializedResult;
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
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.noperator.NewOperatorAssertion.toMaterializedResult;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDatabaseLocalStorageManager
{
    private Handle dummyHandle;
    private File dataDir;
    private LocalStorageManager storageManager;
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        DatabaseLocalStorageManagerConfig config = new DatabaseLocalStorageManagerConfig().setDataDirectory(dataDir);
        storageManager = new DatabaseLocalStorageManager(dbi, config);
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
        executor.shutdownNow();
    }

    @Test
    public void testImportFlow()
            throws IOException
    {
        long shardId = 123;
        assertFalse(storageManager.shardExists(shardId));

        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>of(new NativeColumnHandle("column_7", 7L), new NativeColumnHandle("column_11", 11L));

        List<Page> pages = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .pageBreak()
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .pageBreak()
                .row("alice", 8)
                .row("bob", 9)
                .row("charlie", 10)
                .row("dave", 11)
                .build();

        ColumnFileHandle fileHandles = storageManager.createStagingFileHandles(shardId, columnHandles);
        for (Page page : pages) {
            fileHandles.append(page);
        }
        storageManager.commit(fileHandles);

        assertTrue(storageManager.shardExists(shardId));

        NewAlignmentOperatorFactory factory = new NewAlignmentOperatorFactory(0,
                storageManager.getBlocks(shardId, columnHandles.get(0)),
                storageManager.getBlocks(shardId, columnHandles.get(1)));
        NewOperator operator = factory.createOperator(driverContext);

        // materialize pages to force comparision only on contents and not page boundaries
        MaterializedResult expected = toMaterializedResult(operator.getTupleInfos(), pages);

        NewOperatorAssertion.assertOperatorEquals(operator, expected);
    }

    @Test
    public void testImportEmptySource()
            throws IOException
    {
        long shardId = 456;
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>of(new NativeColumnHandle("column_13", 13L));

        ColumnFileHandle fileHandles = storageManager.createStagingFileHandles(shardId, columnHandles);
        storageManager.commit(fileHandles);

        assertTrue(storageManager.shardExists(shardId));

        assertTrue(Iterables.isEmpty(storageManager.getBlocks(shardId, columnHandles.get(0))));
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
