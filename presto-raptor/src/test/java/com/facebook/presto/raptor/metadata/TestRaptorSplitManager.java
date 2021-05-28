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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder;
import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorSplit;
import com.facebook.presto.raptor.RaptorSplitManager;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.RaptorTableLayoutHandle;
import com.facebook.presto.raptor.RaptorTransactionHandle;
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.raptor.RaptorTableProperties.TABLE_SUPPORTS_DELTA_DELETE;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.shardInfo;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestRaptorSplitManager
{
    private static final ConnectorTableMetadata TEST_TABLE = TableMetadataBuilder.tableMetadataBuilder("demo", "test_table")
            .column("ds", createVarcharType(10))
            .column("foo", createVarcharType(10))
            .column("bar", BigintType.BIGINT)
            .property(TABLE_SUPPORTS_DELTA_DELETE, false)
            .build();

    private Handle dummyHandle;
    private File temporary;
    private RaptorMetadata metadata;
    private RaptorSplitManager raptorSplitManager;
    private ConnectorTableHandle tableHandle;
    private ShardManager shardManager;
    private long tableId;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        DBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
        dbi.registerMapper(new TableColumn.Mapper(functionAndTypeManager));
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        temporary = createTempDir();
        AssignmentLimiter assignmentLimiter = new AssignmentLimiter(ImmutableSet::of, systemTicker(), new MetadataConfig());
        shardManager = new DatabaseShardManager(dbi, new DaoSupplier<>(dbi, ShardDao.class), ImmutableSet::of, assignmentLimiter, systemTicker(), new Duration(0, MINUTES));
        TestingNodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode(new InternalNode(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false));

        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
        metadata = new RaptorMetadata(connectorId.toString(), dbi, shardManager, createTestFunctionAndTypeManager());

        metadata.createTable(SESSION, TEST_TABLE, false);
        tableHandle = metadata.getTableHandle(SESSION, TEST_TABLE.getTable());

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .add(shardInfo(UUID.randomUUID(), nodeName))
                .build();

        tableId = ((RaptorTableHandle) tableHandle).getTableId();

        List<ColumnInfo> columns = metadata.getColumnHandles(SESSION, tableHandle).values().stream()
                .map(RaptorColumnHandle.class::cast)
                .map(ColumnInfo::fromHandle)
                .collect(toList());

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        raptorSplitManager = new RaptorSplitManager(connectorId, nodeSupplier, shardManager, false);
    }

    @AfterMethod
    public void teardown()
            throws IOException
    {
        dummyHandle.close();
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testSanity()
    {
        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        assertEquals(layouts.size(), 1);
        ConnectorTableLayoutResult layout = getOnlyElement(layouts);
        assertInstanceOf(layout.getTableLayout().getHandle(), RaptorTableLayoutHandle.class);

        ConnectorSplitSource splitSource = getSplits(raptorSplitManager, layout);
        int splitCount = 0;
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> splits = getSplits(splitSource, 1000);
            splitCount += splits.size();
            RaptorSplit split = (RaptorSplit) (splits.get(0));
            assertFalse(split.isTableSupportsDeltaDelete());
            assertEquals(split.getColumnTypes(), Optional.empty());
        }
        assertEquals(splitCount, 4);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No host for shard .* found: \\[\\]")
    public void testNoHostForShard()
    {
        deleteShardNodes();

        ConnectorTableLayoutResult layout = getOnlyElement(metadata.getTableLayouts(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty()));
        ConnectorSplitSource splitSource = getSplits(raptorSplitManager, layout);
        getSplits(splitSource, 1000);
    }

    @Test
    public void testAssignRandomNodeWhenBackupAvailable()
            throws URISyntaxException
    {
        TestingNodeManager nodeManager = new TestingNodeManager();
        RaptorConnectorId connectorId = new RaptorConnectorId("raptor");
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        InternalNode node = new InternalNode(UUID.randomUUID().toString(), new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(node);
        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(connectorId, nodeSupplier, shardManager, true);

        deleteShardNodes();

        ConnectorTableLayoutResult layout = getOnlyElement(metadata.getTableLayouts(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty()));
        ConnectorSplitSource partitionSplit = getSplits(raptorSplitManagerWithBackup, layout);
        List<ConnectorSplit> batch = getSplits(partitionSplit, 1);
        assertEquals(getOnlyElement(((RaptorSplit) getOnlyElement(batch)).getAddresses()), node.getHostAndPort());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No nodes available to run query")
    public void testNoNodes()
    {
        deleteShardNodes();

        RaptorSplitManager raptorSplitManagerWithBackup = new RaptorSplitManager(new RaptorConnectorId("fbraptor"), ImmutableSet::of, shardManager, true);
        ConnectorTableLayoutResult layout = getOnlyElement(metadata.getTableLayouts(SESSION, tableHandle, Constraint.alwaysTrue(), Optional.empty()));
        ConnectorSplitSource splitSource = getSplits(raptorSplitManagerWithBackup, layout);
        getSplits(splitSource, 1000);
    }

    private void deleteShardNodes()
    {
        dummyHandle.execute("DELETE FROM shard_nodes");
        dummyHandle.execute(format("UPDATE %s SET node_ids = ''", shardIndexTable(tableId)));
    }

    private static ConnectorSplitSource getSplits(RaptorSplitManager splitManager, ConnectorTableLayoutResult layout)
    {
        ConnectorTransactionHandle transaction = new RaptorTransactionHandle();
        return splitManager.getSplits(transaction, SESSION, layout.getTableLayout().getHandle(), new SplitSchedulingContext(UNGROUPED_SCHEDULING, true, WarningCollector.NOOP));
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, int maxSize)
    {
        return getFutureValue(source.getNextBatch(NOT_PARTITIONED, maxSize)).getSplits();
    }
}
