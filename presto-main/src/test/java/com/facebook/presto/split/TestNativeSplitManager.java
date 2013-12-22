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
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestNativeSplitManager
{
    private static final ConnectorTableMetadata TEST_TABLE = TableMetadataBuilder.tableMetadataBuilder("demo", "test_table")
            .partitionKeyColumn("ds", STRING)
            .column("foo", STRING)
            .column("bar", LONG)
            .build();

    private Handle dummyHandle;
    private File dataDir;
    private NativeSplitManager nativeSplitManager;
    private TableHandle tableHandle;
    private ColumnHandle dsColumnHandle;

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
        metadataManager.addConnectorMetadata("local", "local", new NativeMetadata("native", dbi, shardManager));

        tableHandle = metadataManager.createTable("local", new TableMetadata("local", TEST_TABLE));
        dsColumnHandle = metadataManager.getColumnHandle(tableHandle, "ds").get();

        UUID shardUuid1 = UUID.randomUUID();
        UUID shardUuid2 = UUID.randomUUID();
        UUID shardUuid3 = UUID.randomUUID();
        UUID shardUuid4 = UUID.randomUUID();

        shardManager.commitPartition(
                tableHandle,
                "ds=1",
                ImmutableList.<PartitionKey>of(new NativePartitionKey("ds=1", "ds", STRING, "1")),
                ImmutableMap.<UUID, String>builder()
                        .put(shardUuid1, nodeName)
                        .put(shardUuid2, nodeName)
                        .put(shardUuid3, nodeName)
                        .build());

        shardManager.commitPartition(
                tableHandle,
                "ds=2",
                ImmutableList.<PartitionKey>of(new NativePartitionKey("ds=2", "ds", STRING, "2")),
                ImmutableMap.<UUID, String>builder()
                        .put(shardUuid4, nodeName)
                        .build());

        nativeSplitManager = new NativeSplitManager(nodeManager, shardManager, metadataManager);
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testSanity()
    {
        PartitionResult partitionResult = nativeSplitManager.getPartitions(tableHandle, TupleDomain.all());
        assertEquals(partitionResult.getPartitions().size(), 2);
        assertTrue(partitionResult.getUndeterminedTupleDomain().isAll());

        List<Partition> partitions = partitionResult.getPartitions();
        TupleDomain columnUnionedTupleDomain = partitions.get(0).getTupleDomain().columnWiseUnion(partitions.get(1).getTupleDomain());
        assertEquals(columnUnionedTupleDomain, TupleDomain.withColumnDomains(ImmutableMap.of(dsColumnHandle, Domain.create(SortedRangeSet.of(Range.equal("1"), Range.equal("2")), false))));

        Iterable<Split> splits = nativeSplitManager.getPartitionSplits(tableHandle, partitions);
        assertEquals(Iterables.size(splits), 4);
    }
}
