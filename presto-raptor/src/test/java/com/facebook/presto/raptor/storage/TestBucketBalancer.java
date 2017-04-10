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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.raptor.NodeSupplier;
import com.facebook.presto.raptor.metadata.BucketNode;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.Distribution;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.storage.BucketBalancer.BucketAssignment;
import com.facebook.presto.raptor.storage.BucketBalancer.ClusterState;
import com.facebook.presto.spi.Node;
import com.facebook.presto.testing.TestingNodeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.metadata.Distribution.serializeColumnTypes;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBucketBalancer
{
    private static final List<String> AVAILABLE_WORKERS = ImmutableList.of("node1", "node2", "node3", "node4", "node5");

    private DBI dbi;
    private Handle dummyHandle;
    private ShardManager shardManager;
    private TestingNodeManager nodeManager;
    private MetadataDao metadataDao;
    private BucketBalancer balancer;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dbi.registerMapper(new Distribution.Mapper(typeRegistry));
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);

        metadataDao = dbi.onDemand(MetadataDao.class);
        nodeManager = new TestingNodeManager(AVAILABLE_WORKERS.stream()
                .map(TestBucketBalancer::createTestingNode)
                .collect(Collectors.toList()));

        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        shardManager = createShardManager(dbi, nodeSupplier);
        balancer = new BucketBalancer(nodeSupplier, shardManager, true, new Duration(1, DAYS), true, true, "test");
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
    }

    @Test
    public void testSingleDistributionUnbalanced()
            throws Exception
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 10, 3, 1, 1, 1);

        assertBalancing(balancer, 6);
    }

    @Test
    public void testSingleDistributionSlightlyUnbalanced()
            throws Exception
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 4, 4, 3, 3, 2);

        assertBalancing(balancer, 1);
    }

    @Test
    public void testSingleDistributionBalanced()
            throws Exception
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 4, 3, 3, 3, 3);

        assertBalancing(balancer, 0);
    }

    @Test
    public void testSingleDistributionUnbalancedWithDeadNode()
            throws Exception
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        ImmutableList<String> nodes = ImmutableList.<String>builder().addAll(AVAILABLE_WORKERS).add("node6").build();
        createAssignments(distributionId, nodes, 11, 1, 1, 1, 1, 1);

        assertBalancing(balancer, 8);
    }

    @Test
    public void testSingleDistributionUnbalancedWithNewNode()
            throws Exception
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 12, 1, 1, 1, 1);
        nodeManager.addNode(createTestingNode("node6"));

        assertBalancing(balancer, 9);
    }

    @Test
    public void testMultipleDistributionUnbalanced()
            throws Exception
    {
        long distributionA = createDistribution("distA", 17);
        createBucketedTable("testA", distributionA);
        createAssignments(distributionA, AVAILABLE_WORKERS, 11, 3, 1, 1, 1);

        long distributionB = createDistribution("distB", 10);
        createBucketedTable("testB", distributionB);
        createAssignments(distributionB, AVAILABLE_WORKERS, 8, 2, 0, 0, 0);

        long distributionC = createDistribution("distC", 4);
        createBucketedTable("testC", distributionC);
        createAssignments(distributionC, AVAILABLE_WORKERS, 2, 2, 0, 0, 0);

        assertBalancing(balancer, 15);
    }

    @Test
    public void testMultipleDistributionUnbalancedWithDiskSpace()
            throws Exception
    {
        long distributionA = createDistribution("distA", 4);
        createBucketedTable("testA", distributionA, DataSize.valueOf("4B"));
        createAssignments(distributionA, AVAILABLE_WORKERS, 1, 1, 1, 1, 0);

        long distributionB = createDistribution("distB", 4);
        createBucketedTable("testB", distributionB, DataSize.valueOf("4B"));
        createAssignments(distributionB, AVAILABLE_WORKERS, 1, 1, 1, 0, 1);

        long distributionC = createDistribution("distC", 2);
        createBucketedTable("testC", distributionC, DataSize.valueOf("2B"));
        createAssignments(distributionC, AVAILABLE_WORKERS, 0, 0, 0, 2, 0);

        assertBalancing(balancer, 1);

        assertEquals(balancer.fetchClusterState().getAssignedBytes().values()
                .stream()
                .distinct()
                .count(), 1);
    }

    @Test
    public void testMultipleDistributionUnbalancedWithDiskSpace2()
            throws Exception
    {
        long distributionA = createDistribution("distA", 4);
        createBucketedTable("testA", distributionA, DataSize.valueOf("4B"));
        createAssignments(distributionA, AVAILABLE_WORKERS, 1, 1, 1, 1, 0);

        long distributionB = createDistribution("distB", 4);
        createBucketedTable("testB", distributionB, DataSize.valueOf("4B"));
        createAssignments(distributionB, AVAILABLE_WORKERS, 2, 1, 1, 0, 0);

        assertBalancing(balancer, 1);
    }

    @Test
    public void testMultipleDistributionUnbalancedWorstCase()
            throws Exception
    {
        // we will end up with only one bucket on node1
        long distributionA = createDistribution("distA", 4);
        createBucketedTable("testA", distributionA, DataSize.valueOf("4B"));
        createAssignments(distributionA, AVAILABLE_WORKERS, 4, 0, 0, 0, 0);

        long distributionB = createDistribution("distB", 4);
        createBucketedTable("testB", distributionB, DataSize.valueOf("4B"));
        createAssignments(distributionB, AVAILABLE_WORKERS, 4, 0, 0, 0, 0);

        long distributionC = createDistribution("distC", 4);
        createBucketedTable("testC", distributionC, DataSize.valueOf("4B"));
        createAssignments(distributionC, AVAILABLE_WORKERS, 4, 0, 0, 0, 0);

        long distributionD = createDistribution("distD", 4);
        createBucketedTable("testD", distributionD, DataSize.valueOf("4B"));
        createAssignments(distributionD, AVAILABLE_WORKERS, 4, 0, 0, 0, 0);

        long distributionE = createDistribution("distE", 4);
        createBucketedTable("testE", distributionE, DataSize.valueOf("4B"));
        createAssignments(distributionE, AVAILABLE_WORKERS, 4, 0, 0, 0, 0);

        assertBalancing(balancer, 15);
    }

    private static void assertBalancing(BucketBalancer balancer, int expectedMoves)
    {
        int actualMoves = balancer.balance();
        assertEquals(actualMoves, expectedMoves);

        // check that number of buckets per node is within bounds
        ClusterState clusterState = balancer.fetchClusterState();
        for (Distribution distribution : clusterState.getDistributionAssignments().keySet()) {
            Multiset<String> allocationCounts = HashMultiset.create();
            clusterState.getDistributionAssignments().get(distribution).stream()
                    .map(BucketAssignment::getNodeIdentifier)
                    .forEach(allocationCounts::add);

            double bucketsPerNode = (1.0 * allocationCounts.size()) / clusterState.getActiveNodes().size();
            for (String node : allocationCounts) {
                assertGreaterThanOrEqual(allocationCounts.count(node), (int) Math.floor(bucketsPerNode), node + " has fewer buckets than expected");
                assertLessThanOrEqual(allocationCounts.count(node), (int) Math.ceil(bucketsPerNode), node + " has more buckets than expected");
            }
        }

        // check stability
        assertEquals(balancer.balance(), 0);
    }

    private long createDistribution(String distributionName, int bucketCount)
    {
        MetadataDao dao = dbi.onDemand(MetadataDao.class);
        long distributionId = dao.insertDistribution(distributionName, serializeColumnTypes(ImmutableList.of(BIGINT)), bucketCount);
        shardManager.createBuckets(distributionId, bucketCount);
        return distributionId;
    }

    private long createBucketedTable(String tableName, long distributionId)
    {
        return createBucketedTable(tableName, distributionId, DataSize.valueOf("0B"));
    }

    private long createBucketedTable(String tableName, long distributionId, DataSize compressedSize)
    {
        MetadataDao dao = dbi.onDemand(MetadataDao.class);
        long tableId = dao.insertTable("test", tableName, false, false, distributionId, 0);
        List<ColumnInfo> columnsA = ImmutableList.of(new ColumnInfo(1, BIGINT));
        shardManager.createTable(tableId, columnsA, false, OptionalLong.empty());

        metadataDao.updateTableStats(tableId, 1024, 1024 * 1024 * 1024, compressedSize.toBytes(), compressedSize.toBytes() * 2);
        return tableId;
    }

    private List<BucketNode> createAssignments(long distributionId, List<String> nodes, int... buckets)
    {
        checkArgument(nodes.size() == buckets.length);
        ImmutableList.Builder<BucketNode> assignments = ImmutableList.builder();
        int bucketNumber = 0;
        for (int i = 0; i < buckets.length; i++) {
            for (int j = 0; j < buckets[i]; j++) {
                shardManager.updateBucketAssignment(distributionId, bucketNumber, nodes.get(i));
                assignments.add(bucketNode(bucketNumber, nodes.get(i)));

                bucketNumber++;
            }
        }
        return assignments.build();
    }

    private static BucketNode bucketNode(int bucketNumber, String nodeIdentifier)
    {
        return new BucketNode(bucketNumber, nodeIdentifier);
    }

    private static Node createTestingNode(String nodeIdentifier)
    {
        return new PrestoNode(nodeIdentifier, URI.create("http://test"), NodeVersion.UNKNOWN, false);
    }
}
