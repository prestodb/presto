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
package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.ScheduleResult;
import com.facebook.presto.execution.scheduler.SourceScheduler;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestDynamicLifespanScheduler
{
    private static final int BUCKET_COUNT = 10;
    private static final int TASK_COUNT = 2;

    private static final InternalNode node1 = getInternalNode("1");
    private static final InternalNode node2 = getInternalNode("2");
    private static final InternalNode node3 = getInternalNode("3");

    @Test
    public void testSchedule()
    {
        LifespanScheduler lifespanScheduler = getLifespanScheduler();
        TestingSourceScheduler sourceScheduler = new TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);
        lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();

        while (!lifespanScheduler.allLifespanExecutionFinished()) {
            lifespanScheduler.schedule(sourceScheduler);
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
            sourceScheduler.getLastStartedLifespans().clear();
        }
    }

    @Test
    public void testRetry()
    {
        LifespanScheduler lifespanScheduler = getLifespanScheduler();
        TestingSourceScheduler sourceScheduler = new TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);
        lifespanScheduler.onLifespanExecutionFinished(ImmutableList.of(sourceScheduler.getLastStartedLifespans().get(1)));
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();

        lifespanScheduler.onTaskFailed(0, ImmutableList.of(sourceScheduler));
        assertEquals(sourceScheduler.getLastRewoundLifespans().size(), 1);
        sourceScheduler.getLastRewoundLifespans().clear();

        while (!lifespanScheduler.allLifespanExecutionFinished()) {
            lifespanScheduler.schedule(sourceScheduler);
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 1);
            sourceScheduler.getLastStartedLifespans().clear();
        }
    }

    @Test(timeOut = 10_000)
    public void testRetryLastLifespan()
    {
        LifespanScheduler lifespanScheduler = getLifespanScheduler();
        TestingSourceScheduler sourceScheduler = new TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);

        for (int i = 0; i < BUCKET_COUNT / TASK_COUNT - 1; i++) {
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            sourceScheduler.getLastStartedLifespans().clear();
            lifespanScheduler.schedule(sourceScheduler);
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        }

        lifespanScheduler.onLifespanExecutionFinished(ImmutableList.of(sourceScheduler.getLastStartedLifespans().get(1)));
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();
        lifespanScheduler.onTaskFailed(0, ImmutableList.of(sourceScheduler));
        assertEquals(sourceScheduler.getLastRewoundLifespans().size(), 1);
        sourceScheduler.getLastRewoundLifespans().clear();

        lifespanScheduler.schedule(sourceScheduler);
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 1);
        lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
    }

    @Test
    public void testAffinitySchedule()
    {
        BucketNodeMap bucketNodeMap = new DynamicBucketNodeMap(
                split -> ((TestDynamicLifespanScheduler.TestSplit) split.getConnectorSplit()).getBucketNumber(),
                BUCKET_COUNT,
                ImmutableList.of(node1, node2, node1, node2, node1, node2, node1, node2, node1, node2));
        LifespanScheduler lifespanScheduler = getAffinityLifespanScheduler(bucketNodeMap);
        TestDynamicLifespanScheduler.TestingSourceScheduler sourceScheduler = new TestDynamicLifespanScheduler.TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);
        lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();

        while (!lifespanScheduler.allLifespanExecutionFinished()) {
            lifespanScheduler.schedule(sourceScheduler);
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
            sourceScheduler.getLastStartedLifespans().clear();
        }
    }

    @Test
    public void testAffinityRetry()
    {
        BucketNodeMap bucketNodeMap = new DynamicBucketNodeMap(
                split -> ((TestDynamicLifespanScheduler.TestSplit) split.getConnectorSplit()).getBucketNumber(),
                BUCKET_COUNT,
                ImmutableList.of(node1, node2, node1, node2, node1, node2, node1, node2, node1, node2));
        LifespanScheduler lifespanScheduler = getAffinityLifespanScheduler(bucketNodeMap);
        TestDynamicLifespanScheduler.TestingSourceScheduler sourceScheduler = new TestDynamicLifespanScheduler.TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);

        lifespanScheduler.onLifespanExecutionFinished(ImmutableList.of(sourceScheduler.getLastStartedLifespans().get(1)));
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();

        lifespanScheduler.onTaskFailed(0, ImmutableList.of(sourceScheduler));
        assertEquals(sourceScheduler.getLastRewoundLifespans().size(), 1);
        sourceScheduler.getLastRewoundLifespans().clear();

        while (!lifespanScheduler.allLifespanExecutionFinished()) {
            lifespanScheduler.schedule(sourceScheduler);
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 1);
            sourceScheduler.getLastStartedLifespans().clear();
        }
    }

    @Test
    public void testAffinityScheduleLocality()
    {
        BucketNodeMap bucketNodeMap = new DynamicBucketNodeMap(
                split -> ((TestDynamicLifespanScheduler.TestSplit) split.getConnectorSplit()).getBucketNumber(),
                BUCKET_COUNT,
                ImmutableList.of(node1, node3, node1, node3, node1, node3, node1, node3, node1, node3));
        LifespanScheduler lifespanScheduler = getAffinityLifespanScheduler(bucketNodeMap);
        TestDynamicLifespanScheduler.TestingSourceScheduler sourceScheduler = new TestDynamicLifespanScheduler.TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);
        assertEquals(bucketNodeMap.getAssignedNode(0).get(), node1);
        // bucket 1 is already scheduled, thus its assignedNode is changed
        assertEquals(bucketNodeMap.getAssignedNode(1).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(2).get(), node1);
        // bucket 3 is not scheduled yet, thus its assignedNode remains
        assertEquals(bucketNodeMap.getAssignedNode(3).get(), node3);
        assertEquals(bucketNodeMap.getAssignedNode(4).get(), node1);
        assertEquals(bucketNodeMap.getAssignedNode(5).get(), node3);
        assertEquals(bucketNodeMap.getAssignedNode(6).get(), node1);
        assertEquals(bucketNodeMap.getAssignedNode(7).get(), node3);
        assertEquals(bucketNodeMap.getAssignedNode(8).get(), node1);
        assertEquals(bucketNodeMap.getAssignedNode(9).get(), node3);

        lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();

        while (!lifespanScheduler.allLifespanExecutionFinished()) {
            lifespanScheduler.schedule(sourceScheduler);
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
            sourceScheduler.getLastStartedLifespans().clear();
        }
        assertEquals(bucketNodeMap.getAssignedNode(0).get(), node1);
        // bucket 1 is already scheduled, thus its assignedNode is changed
        assertEquals(bucketNodeMap.getAssignedNode(1).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(2).get(), node1);
        // bucket 3 is not scheduled yet, thus its assignedNode remains
        assertEquals(bucketNodeMap.getAssignedNode(3).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(4).get(), node1);
        assertEquals(bucketNodeMap.getAssignedNode(5).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(6).get(), node1);
        assertEquals(bucketNodeMap.getAssignedNode(7).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(8).get(), node1);
        assertEquals(bucketNodeMap.getAssignedNode(9).get(), node2);
    }

    @Test
    public void testAffinityScheduleFailedLocality()
    {
        BucketNodeMap bucketNodeMap = new DynamicBucketNodeMap(
                split -> ((TestDynamicLifespanScheduler.TestSplit) split.getConnectorSplit()).getBucketNumber(),
                BUCKET_COUNT,
                ImmutableList.of(node1, node2, node1, node2, node1, node2, node1, node2, node1, node2));
        LifespanScheduler lifespanScheduler = getAffinityLifespanScheduler(bucketNodeMap);
        TestDynamicLifespanScheduler.TestingSourceScheduler sourceScheduler = new TestDynamicLifespanScheduler.TestingSourceScheduler();
        lifespanScheduler.scheduleInitial(sourceScheduler);

        lifespanScheduler.onLifespanExecutionFinished(ImmutableList.of(sourceScheduler.getLastStartedLifespans().get(1)));
        assertEquals(sourceScheduler.getLastStartedLifespans().size(), 2);
        sourceScheduler.getLastStartedLifespans().clear();

        lifespanScheduler.onTaskFailed(0, ImmutableList.of(sourceScheduler));
        assertEquals(sourceScheduler.getLastRewoundLifespans().size(), 1);
        sourceScheduler.getLastRewoundLifespans().clear();

        while (!lifespanScheduler.allLifespanExecutionFinished()) {
            lifespanScheduler.schedule(sourceScheduler);
            lifespanScheduler.onLifespanExecutionFinished(sourceScheduler.getLastStartedLifespans());
            assertEquals(sourceScheduler.getLastStartedLifespans().size(), 1);
            sourceScheduler.getLastStartedLifespans().clear();
        }

        assertEquals(bucketNodeMap.getAssignedNode(0).get(), node2);
        // bucket 1 is already scheduled, thus its assignedNode is changed
        assertEquals(bucketNodeMap.getAssignedNode(1).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(2).get(), node2);
        // bucket 3 is not scheduled yet, thus its assignedNode remains
        assertEquals(bucketNodeMap.getAssignedNode(3).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(4).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(5).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(6).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(7).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(8).get(), node2);
        assertEquals(bucketNodeMap.getAssignedNode(9).get(), node2);
    }

    private static LifespanScheduler getAffinityLifespanScheduler(BucketNodeMap bucketNodeMap)
    {
        return new DynamicLifespanScheduler(
                bucketNodeMap,
                ImmutableList.of(node1, node2),
                IntStream.range(0, 10).mapToObj(TestDynamicLifespanScheduler.TestPartitionHandle::new).collect(toImmutableList()),
                OptionalInt.of(1));
    }

    private static LifespanScheduler getLifespanScheduler()
    {
        return new DynamicLifespanScheduler(
                new DynamicBucketNodeMap(split -> ((TestSplit) split.getConnectorSplit()).getBucketNumber(), BUCKET_COUNT),
                ImmutableList.of(getInternalNode("1"), getInternalNode("2")),
                IntStream.range(0, 10).mapToObj(TestPartitionHandle::new).collect(toImmutableList()),
                OptionalInt.of(1));
    }

    private static InternalNode getInternalNode(String id)
    {
        return new InternalNode(id, URI.create(id), new NodeVersion("test"), false);
    }

    private static class TestSplit
            implements ConnectorSplit
    {
        private final int bucketNumber;

        private TestSplit(int bucketNumber)
        {
            this.bucketNumber = bucketNumber;
        }

        public int getBucketNumber()
        {
            return bucketNumber;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    private static class TestPartitionHandle
            extends ConnectorPartitionHandle
    {
        private final int bucket;

        public TestPartitionHandle(int bucket)
        {
            this.bucket = bucket;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestPartitionHandle)) {
                return false;
            }
            TestPartitionHandle that = (TestPartitionHandle) o;
            return bucket == that.bucket;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(bucket);
        }
    }

    public class TestingSourceScheduler
            implements SourceScheduler
    {
        private final List<Lifespan> lastStartedLifespans = new ArrayList<>();
        private final List<Lifespan> lastRewoundLifespans = new ArrayList<>();

        public ScheduleResult schedule()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
        {
            lastStartedLifespans.add(lifespan);
        }

        @Override
        public void rewindLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
        {
            lastRewoundLifespans.add(lifespan);
        }

        @Override
        public List<Lifespan> drainCompletelyScheduledLifespans()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyAllLifespansFinishedExecution()
        {
            throw new UnsupportedOperationException();
        }

        public List<Lifespan> getLastStartedLifespans()
        {
            return lastStartedLifespans;
        }

        public List<Lifespan> getLastRewoundLifespans()
        {
            return lastRewoundLifespans;
        }
    }
}
