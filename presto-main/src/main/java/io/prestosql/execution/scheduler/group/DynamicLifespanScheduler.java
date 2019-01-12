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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * See {@link LifespanScheduler} about thread safety
 */
public class DynamicLifespanScheduler
        implements LifespanScheduler
{
    private final BucketNodeMap bucketNodeMap;
    private final List<Node> allNodes;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private final OptionalInt concurrentLifespansPerTask;

    private final IntListIterator driverGroups;

    // initialScheduled does not need to be guarded because this object
    // is safely published after its mutation.
    private boolean initialScheduled;
    // Write to newDriverGroupReady field is guarded. Read of the reference
    // is either guarded, or is guaranteed to happen in the same thread as the write.
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();

    @GuardedBy("this")
    private final List<Lifespan> recentlyCompletedDriverGroups = new ArrayList<>();

    public DynamicLifespanScheduler(BucketNodeMap bucketNodeMap, List<Node> allNodes, List<ConnectorPartitionHandle> partitionHandles, OptionalInt concurrentLifespansPerTask)
    {
        this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        this.allNodes = requireNonNull(allNodes, "allNodes is null");
        this.partitionHandles = unmodifiableList(new ArrayList<>(
                requireNonNull(partitionHandles, "partitionHandles is null")));

        this.concurrentLifespansPerTask = requireNonNull(concurrentLifespansPerTask, "concurrentLifespansPerTask is null");
        concurrentLifespansPerTask.ifPresent(lifespansPerTask -> checkArgument(lifespansPerTask >= 1, "concurrentLifespansPerTask must be great or equal to 1 if present"));

        int bucketCount = partitionHandles.size();
        verify(bucketCount > 0);
        this.driverGroups = new IntArrayList(IntStream.range(0, bucketCount).toArray()).iterator();
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled);
        initialScheduled = true;

        int driverGroupsScheduledPerTask = 0;
        while (driverGroups.hasNext()) {
            for (int i = 0; i < allNodes.size() && driverGroups.hasNext(); i++) {
                int driverGroupId = driverGroups.nextInt();
                checkState(!bucketNodeMap.getAssignedNode(driverGroupId).isPresent());
                bucketNodeMap.assignBucketToNode(driverGroupId, allNodes.get(i));
                scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
            }

            driverGroupsScheduledPerTask++;
            if (concurrentLifespansPerTask.isPresent() && driverGroupsScheduledPerTask == concurrentLifespansPerTask.getAsInt()) {
                break;
            }
        }

        if (!driverGroups.hasNext()) {
            scheduler.noMoreLifespans();
        }
    }

    @Override
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

    @Override
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
            if (!driverGroups.hasNext()) {
                break;
            }
            int driverGroupId = driverGroups.nextInt();

            Node nodeForCompletedDriverGroup = bucketNodeMap.getAssignedNode(driverGroup.getId()).orElseThrow(IllegalStateException::new);
            bucketNodeMap.assignBucketToNode(driverGroupId, nodeForCompletedDriverGroup);
            scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
        }

        if (!driverGroups.hasNext()) {
            scheduler.noMoreLifespans();
        }
        return newDriverGroupReady;
    }
}
