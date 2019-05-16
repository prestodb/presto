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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.raptorx.metadata.BucketNode;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DistributionInfo;
import com.facebook.presto.raptorx.metadata.Metadata;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.RaptorNodeSupplier;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.storage.BucketBalancer.BucketAssignment;
import com.facebook.presto.raptorx.storage.BucketBalancer.ClusterState;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.raptorx.util.TestingDatabase;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.facebook.presto.raptorx.CreateDistributionProcedure.bucketNodes;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertLessThanOrEqual;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBucketBalancer
{
    private static final List<String> AVAILABLE_WORKERS = ImmutableList.of("node1", "node2", "node3", "node4", "node5");

    private TestingDatabase database;
    private TestingNodeManager nodeManager;
    private BucketBalancer balancer;
    private Metadata metadata;
    private NodeIdCache nodeIdCache;
    private static Map<String, Long> nodeName2Id;
    private TransactionWriter transactionWriter;

    @BeforeMethod
    public void setup()
    {
        database = new TestingDatabase();
        new SchemaCreator(database).create();
        TestingEnvironment environment = new TestingEnvironment(database);
        metadata = environment.getMetadata();
        transactionWriter = environment.getTransactionWriter();
        nodeIdCache = environment.getNodeIdCache();
        nodeName2Id = nodeIdCache.getNodeIds(new HashSet<>(AVAILABLE_WORKERS));

        nodeManager = new TestingNodeManager(AVAILABLE_WORKERS.stream()
                .map(TestBucketBalancer::createTestingNode)
                .collect(Collectors.toList()));

        RaptorNodeSupplier nodeSupplier = new RaptorNodeSupplier(nodeManager, nodeIdCache);
        balancer = new BucketBalancer(nodeSupplier, metadata, true, new Duration(1, DAYS));
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        if (database != null) {
            database.close();
        }
    }

    @Test
    public void testSingleDistributionUnbalanced()
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 10, 3, 1, 1, 1);
        assertBalancing(balancer, 6);
    }

    @Test
    public void testSingleDistributionSlightlyUnbalanced()
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 4, 4, 3, 3, 2);

        assertBalancing(balancer, 1);
    }

    @Test
    public void testSingleDistributionBalanced()
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        createAssignments(distributionId, AVAILABLE_WORKERS, 4, 3, 3, 3, 3);

        assertBalancing(balancer, 0);
    }

    @Test
    public void testSingleDistributionUnbalancedWithDeadNode()
    {
        long distributionId = createDistribution("distA", 16);
        createBucketedTable("testA", distributionId);
        createBucketedTable("testB", distributionId);

        ImmutableList<String> nodes = ImmutableList.<String>builder().addAll(AVAILABLE_WORKERS).add("node6").build();
        createAssignments(distributionId, nodes, 11, 1, 1, 1, 1, 1);

        assertBalancing(balancer, 9);
    }

    @Test
    public void testSingleDistributionUnbalancedWithNewNode()
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
        for (DistributionInfo distribution : clusterState.getDistributionAssignments().keySet()) {
            Multiset<Long> allocationCounts = HashMultiset.create();
            clusterState.getDistributionAssignments().get(distribution).stream()
                    .map(BucketAssignment::getNodeId)
                    .forEach(allocationCounts::add);

            double bucketsPerNode = (1.0 * allocationCounts.size()) / clusterState.getActiveNodes().size();
            for (long node : allocationCounts) {
                assertGreaterThanOrEqual(allocationCounts.count(node), (int) Math.floor(bucketsPerNode), node + " has fewer buckets than expected");
                assertLessThanOrEqual(allocationCounts.count(node), (int) Math.ceil(bucketsPerNode), node + " has more buckets than expected");
            }
        }

        // check stability
        assertEquals(balancer.balance(), 0);
    }

    private long createDistribution(String distributionName, int bucketCount)
    {
        long distributionId = metadata.createDistribution(Optional.of(distributionName), ImmutableList.of(BIGINT), bucketNodes(ImmutableSet.copyOf(nodeName2Id.values()), toIntExact(bucketCount)));
        return distributionId;
    }

    private long createBucketedTable(String tableName, long distributionId)
    {
        return createBucketedTable(tableName, distributionId, DataSize.valueOf("0B"));
    }

    private long createBucketedTable(String tableName, long distributionId, DataSize compressedSize)
    {
        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());
        long tableId = metadata.nextTableId();
        transaction.createTable(
                tableId,
                metadata.nextSchemaId(),
                tableName,
                distributionId,
                OptionalLong.of(20),
                CompressionType.ZSTD,
                System.currentTimeMillis(),
                Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(10, "orderkey", BIGINT, 1, OptionalInt.of(1)))
                        .add(column(20, "orderdate", DATE, 2, OptionalInt.empty()))
                        .add(column(30, "orderstatus", createVarcharType(3), 3, OptionalInt.empty()))
                        .build(), false);
        transactionWriter.write(transaction.getActions(), OptionalLong.empty());
        return tableId;
    }

    private List<BucketNode> createAssignments(long distributionId, List<String> nodes, int... buckets)
    {
        checkArgument(nodes.size() == buckets.length);
        ImmutableList.Builder<BucketNode> assignments = ImmutableList.builder();
        int bucketNumber = 0;
        if (buckets.length > AVAILABLE_WORKERS.size()) {
            nodeName2Id = nodeIdCache.getNodeIds(ImmutableSet.copyOf(nodes));
        }
        for (int i = 0; i < buckets.length; i++) {
            for (int j = 0; j < buckets[i]; j++) {
                metadata.updateBucketAssignment(distributionId, bucketNumber, nodeName2Id.get((nodes.get(i))));
                assignments.add(bucketNode(bucketNumber, nodes.get(i)));

                bucketNumber++;
            }
        }
        return assignments.build();
    }

    private static BucketNode bucketNode(int bucketNumber, String nodeIdentifier)
    {
        return new BucketNode(bucketNumber, nodeName2Id.get(nodeIdentifier));
    }

    private static Node createTestingNode(String nodeIdentifier)
    {
        return new InternalNode(nodeIdentifier, URI.create("http://test"), NodeVersion.UNKNOWN, false);
    }

    private static ColumnInfo column(long columnId, String name, Type type, int ordinal, OptionalInt bucketOrdinal)
    {
        return new ColumnInfo(columnId, name, type, Optional.empty(), ordinal, bucketOrdinal, OptionalInt.empty());
    }
}
