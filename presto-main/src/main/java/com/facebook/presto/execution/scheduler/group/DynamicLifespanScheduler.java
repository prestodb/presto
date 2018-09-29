package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.SourceScheduler;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
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
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicLifespanScheduler
        implements LifespanScheduler
{
    private final BucketedSplitAssignment bucketedSplitAssignment;
    private final List<Node> allNodes;
    private final List<ConnectorPartitionHandle> partitionHandles;

    private final IntListIterator driverGroups;

    private boolean initialScheduled;
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();
    @GuardedBy("this")
    private final List<Lifespan> recentlyCompletedDriverGroups = new ArrayList<>();

    public DynamicLifespanScheduler(BucketedSplitAssignment bucketedSplitAssignment, List<Node> allNodes, List<ConnectorPartitionHandle> partitionHandles)
    {
        this.bucketedSplitAssignment = bucketedSplitAssignment;
        this.allNodes = allNodes;
        this.partitionHandles = requireNonNull(partitionHandles, "partitionHandles is null");

        int bucketCount = partitionHandles.size();
        this.driverGroups = new IntArrayList(IntStream.range(0, bucketCount).toArray()).iterator();
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled);
        initialScheduled = true;

        for (int i = 0; i < allNodes.size() && driverGroups.hasNext(); i++) {
            int driverGroupId = driverGroups.nextInt();
            checkState(!bucketedSplitAssignment.getAssignedNode(driverGroupId).isPresent());
            bucketedSplitAssignment.assignOrChangeBucketToNode(driverGroupId, allNodes.get(i));
            scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
        }
    }

    @Override
    public void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups)
    {
        checkState(initialScheduled);

        synchronized (this) {
            for (Lifespan newlyCompletedDriverGroup : newlyCompletedDriverGroups) {
                checkArgument(!newlyCompletedDriverGroup.isTaskWide());
                recentlyCompletedDriverGroups.add(newlyCompletedDriverGroup);
            }
            newDriverGroupReady.set(null);
        }
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
            checkState(!bucketedSplitAssignment.getAssignedNode(driverGroupId).isPresent());

            Node nodeForCompletedDriverGroup = bucketedSplitAssignment.getAssignedNode(driverGroup.getId()).get();
            bucketedSplitAssignment.assignOrChangeBucketToNode(driverGroupId, nodeForCompletedDriverGroup);
            scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
        }

        return newDriverGroupReady;
    }
}

