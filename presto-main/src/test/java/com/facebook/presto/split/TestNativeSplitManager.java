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
package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static org.testng.Assert.assertEquals;

public class TestNativeSplitManager
{
    private static final ConnectorTableMetadata TEST_TABLE = TableMetadataBuilder.tableMetadataBuilder("demo", "test_table")
            .partitionKeyColumn("ds", STRING)
            .column("foo", STRING)
            .column("bar", LONG)
            .build();

    private static final Session session = new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, null, null, System.currentTimeMillis());

    private Handle dummyHandle;
    private File dataDir;
    private NativeSplitManager nativeSplitManager;
    private SplitManager splitManager;
    private TableHandle tableHandle;
    private ColumnHandle dsColumnHandle;
    private ColumnHandle fooColumnHandle;
    private Map<Symbol, ColumnHandle> symbols;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        ShardManager shardManager = new DatabaseShardManager(dbi);
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();

        String nodeName = UUID.randomUUID().toString();
        nodeManager.addNode("native", new Node(nodeName, new URI("http://127.0.0.1/"), NodeVersion.UNKNOWN));

        MetadataManager metadataManager = new MetadataManager();
        metadataManager.addConnectorMetadata("local", "local", new NativeMetadata("native", dbi));

        tableHandle = metadataManager.createTable("local", new TableMetadata("local", TEST_TABLE));
        dsColumnHandle = metadataManager.getColumnHandle(tableHandle, "ds").get();
        fooColumnHandle = metadataManager.getColumnHandle(tableHandle, "foo").get();
        symbols = ImmutableMap.<Symbol, ColumnHandle>of(new Symbol("foo"), fooColumnHandle, new Symbol("ds"), dsColumnHandle);

        long shardId1 = shardManager.allocateShard(tableHandle);
        long shardId2 = shardManager.allocateShard(tableHandle);
        long shardId3 = shardManager.allocateShard(tableHandle);
        long shardId4 = shardManager.allocateShard(tableHandle);

        shardManager.commitPartition(tableHandle, "ds=1", ImmutableList.<PartitionKey>of(new NativePartitionKey("ds=1", "ds", ColumnType.STRING, "1")), ImmutableMap.of(shardId1, nodeName,
                shardId2, nodeName,
                shardId3, nodeName));
        shardManager.commitPartition(tableHandle, "ds=2", ImmutableList.<PartitionKey>of(new NativePartitionKey("ds=2", "ds", ColumnType.STRING, "2")), ImmutableMap.of(shardId4, nodeName));

        nativeSplitManager = new NativeSplitManager(nodeManager, shardManager, metadataManager);
        splitManager = new SplitManager(metadataManager, ImmutableSet.<ConnectorSplitManager>of(nativeSplitManager));
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testNoPruning()
    {
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Object>>of(ImmutableMap.<ColumnHandle, Object>of()));
        assertEquals(partitions.size(), 2);

        DataSource dataSource = splitManager.getSplits(session, tableHandle, BooleanLiteral.TRUE_LITERAL, BooleanLiteral.TRUE_LITERAL, Predicates.<Partition>alwaysTrue(), ImmutableMap.<Symbol, ColumnHandle>of(new Symbol("ds"), dsColumnHandle));
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        assertEquals(splits.size(), 4);
    }

    @Test
    public void testPruneNoMatch()
    {
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Object>>of(ImmutableMap.<ColumnHandle, Object>of(dsColumnHandle, "foo")));
        assertEquals(partitions.size(), 2);

        // ds=3. No partition will match this.
        Expression nonMatching = new ComparisonExpression(Type.EQUAL, new QualifiedNameReference(new QualifiedName("ds")), new StringLiteral("3"));
        DataSource dataSource = splitManager.getSplits(session, tableHandle, BooleanLiteral.TRUE_LITERAL, nonMatching, Predicates.<Partition>alwaysTrue(), symbols);
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        // no splits found
        assertEquals(splits.size(), 0);
    }

    @Test
    public void testPruneMatch()
    {
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Object>>of(ImmutableMap.<ColumnHandle, Object>of(dsColumnHandle, "1")));
        assertEquals(partitions.size(), 2);

        // ds=1. One partition with three splits will match this.
        Expression nonMatching = new ComparisonExpression(Type.EQUAL, new QualifiedNameReference(new QualifiedName("ds")), new StringLiteral("1"));
        DataSource dataSource = splitManager.getSplits(session, tableHandle, BooleanLiteral.TRUE_LITERAL, nonMatching, Predicates.<Partition>alwaysTrue(), symbols);
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        // three splits found
        assertEquals(splits.size(), 3);
    }

    @Test
    public void testNoPruneUnknown()
    {
        List<Partition> partitions = splitManager.getPartitions(tableHandle, Optional.<Map<ColumnHandle, Object>>of(ImmutableMap.<ColumnHandle, Object>of(dsColumnHandle, "foo")));
        assertEquals(partitions.size(), 2);

        // foo=bar. Not a prunable column
        Expression nonMatching = new ComparisonExpression(Type.EQUAL, new QualifiedNameReference(new QualifiedName("foo")), new StringLiteral("bar"));
        DataSource dataSource = splitManager.getSplits(session, tableHandle, BooleanLiteral.TRUE_LITERAL, nonMatching, Predicates.<Partition>alwaysTrue(), symbols);
        List<Split> splits = ImmutableList.copyOf(dataSource.getSplits());
        // all splits found
        assertEquals(splits.size(), 4);
    }
}
