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
package com.facebook.presto.metadata;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.AlignmentOperator.AlignmentOperatorFactory;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorAssertion;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.TaskContext;
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
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.metadata.DatabaseLocalStorageManager.getShardPath;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
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
        UUID shardUuid = UUID.randomUUID();
        assertFalse(storageManager.shardExists(shardUuid));

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

        ColumnFileHandle fileHandles = storageManager.createStagingFileHandles(shardUuid, columnHandles);
        for (Page page : pages) {
            fileHandles.append(page);
        }
        storageManager.commit(fileHandles);

        assertTrue(storageManager.shardExists(shardUuid));

        AlignmentOperatorFactory factory = new AlignmentOperatorFactory(0,
                storageManager.getBlocks(shardUuid, columnHandles.get(0)),
                storageManager.getBlocks(shardUuid, columnHandles.get(1)));
        Operator operator = factory.createOperator(driverContext);

        // materialize pages to force comparision only on contents and not page boundaries
        MaterializedResult expected = toMaterializedResult(operator.getTupleInfos(), pages);

        OperatorAssertion.assertOperatorEquals(operator, expected);
    }

    @Test
    public void testImportEmptySource()
            throws IOException
    {
        UUID shardUuid = UUID.randomUUID();
        List<ColumnHandle> columnHandles = ImmutableList.<ColumnHandle>of(new NativeColumnHandle("column_13", 13L));

        ColumnFileHandle fileHandles = storageManager.createStagingFileHandles(shardUuid, columnHandles);
        storageManager.commit(fileHandles);

        assertTrue(storageManager.shardExists(shardUuid));

        assertTrue(Iterables.isEmpty(storageManager.getBlocks(shardUuid, columnHandles.get(0))));
    }

    @Test
    public void testShardPath()
    {
        UUID uuid = UUID.fromString("db298a0c-e968-4d5a-8e58-b1021c7eab2c");
        File expected = getShardPath(new File("/data/test"), uuid);
        File actual = new File("/data/test/db/29/8a/db298a0c-e968-4d5a-8e58-b1021c7eab2c");
        assertEquals(expected, actual);
    }
}
