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
package io.prestosql.execution.scheduler.group;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.scheduler.BucketNodeMap;
import io.prestosql.execution.scheduler.SourceScheduler;
import io.prestosql.spi.Node;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Objects.requireNonNull;

/**
 * See {@link LifespanScheduler} about thread safety
 */
public class FixedLifespanScheduler
        implements LifespanScheduler
{
    private final Int2ObjectMap<Node> driverGroupToNodeMap;
    private final Map<Node, IntListIterator> nodeToDriverGroupsMap;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private final OptionalInt concurrentLifespansPerTask;

    private boolean initialScheduled;
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();
    @GuardedBy("this")
    private final List<Lifespan> recentlyCompletedDriverGroups = new ArrayList<>();
    private int totalDriverGroupsScheduled;

    public FixedLifespanScheduler(BucketNodeMap bucketNodeMap, List<ConnectorPartitionHandle> partitionHandles, OptionalInt concurrentLifespansPerTask)
    {
        checkArgument(!partitionHandles.equals(ImmutableList.of(NOT_PARTITIONED)));
        checkArgument(partitionHandles.size() == bucketNodeMap.getBucketCount());

        Map<Node, IntList> nodeToDriverGroupMap = new HashMap<>();
        Int2ObjectMap<Node> driverGroupToNodeMap = new Int2ObjectOpenHashMap<>();
        for (int bucket = 0; bucket < bucketNodeMap.getBucketCount(); bucket++) {
            Node node = bucketNodeMap.getAssignedNode(bucket).get();
            nodeToDriverGroupMap.computeIfAbsent(node, key -> new IntArrayList()).add(bucket);
            driverGroupToNodeMap.put(bucket, node);
        }

        this.driverGroupToNodeMap = driverGroupToNodeMap;
        this.nodeToDriverGroupsMap = nodeToDriverGroupMap.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().iterator()));
        this.partitionHandles = requireNonNull(partitionHandles, "partitionHandles is null");
        if (concurrentLifespansPerTask.isPresent()) {
            checkArgument(concurrentLifespansPerTask.getAsInt() >= 1, "concurrentLifespansPerTask must be great or equal to 1 if present");
        }
        this.concurrentLifespansPerTask = requireNonNull(concurrentLifespansPerTask, "concurrentLifespansPerTask is null");
    }

    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled);
        initialScheduled = true;

        for (Map.Entry<Node, IntListIterator> entry : nodeToDriverGroupsMap.entrySet()) {
            IntListIterator driverGroupsIterator = entry.getValue();
            int driverGroupsScheduled = 0;
            while (driverGroupsIterator.hasNext()) {
                int driverGroupId = driverGroupsIterator.nextInt();
                scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));

                totalDriverGroupsScheduled++;
                driverGroupsScheduled++;
                if (concurrentLifespansPerTask.isPresent() && driverGroupsScheduled == concurrentLifespansPerTask.getAsInt()) {
                    break;
                }
            }
        }

        verify(totalDriverGroupsScheduled <= driverGroupToNodeMap.size());
        if (totalDriverGroupsScheduled == driverGroupToNodeMap.size()) {
            scheduler.noMoreLifespans();
        }
    }

    public void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups)
    {
        checkState(initialScheduled);

        SettableFuture<?> newDriverGroupReady;
        synchronized (this) {
            for (Lifespan newlyCompletedDriverGroup : newlyCompletedDriverGroups) {
                checkArgument(!newlyCompletedDriverGroup.isTaskWide());
                recentlyCompletedDriverGroups.add(newlyCompletedDriverGroup);
            }
            newDriverGroupReady = this.newDriverGroupReady;
        }
        newDriverGroupReady.set(null);
    }

    public SettableFuture schedule(SourceScheduler scheduler)
    {
        // Return a new future even if newDriverGroupReady has not finished.
        // Returning the same SettableFuture instance could lead to ListenableFuture retaining too many listener objects.

        checkState(initialScheduled);

        List<Lifespan> recentlyCompletedDriverGroups;
        synchronized (this) {
            recentlyCompletedDriverGroups = ImmutableList.copyOf(this.recentlyCompletedDriverGroups);
            this.recentlyCompletedDriverGroups.clear();
            newDriverGroupReady = SettableFuture.create();
        }

        for (Lifespan driverGroup : recentlyCompletedDriverGroups) {
            IntListIterator driverGroupsIterator = nodeToDriverGroupsMap.get(driverGroupToNodeMap.get(driverGroup.getId()));
            if (!driverGroupsIterator.hasNext()) {
                continue;
            }
            int driverGroupId = driverGroupsIterator.nextInt();
            scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
            totalDriverGroupsScheduled++;
        }

        verify(totalDriverGroupsScheduled <= driverGroupToNodeMap.size());
        if (totalDriverGroupsScheduled == driverGroupToNodeMap.size()) {
            scheduler.noMoreLifespans();
        }

        return newDriverGroupReady;
    }
}
