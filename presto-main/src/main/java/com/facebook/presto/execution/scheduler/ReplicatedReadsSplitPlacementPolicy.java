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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class ReplicatedReadsSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    private final NodeSelector nodeSelector;
    private final BucketNodeMap bucketNodeMap;
    private final List<InternalNode> allNodes;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;

    public ReplicatedReadsSplitPlacementPolicy(
            NodeSelector nodeSelector,
            List<InternalNode> allNodes,
            BucketNodeMap bucketNodeMap,
            Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSelector = nodeSelector;
        this.allNodes = allNodes;
        this.bucketNodeMap = bucketNodeMap;
        this.remoteTasks = remoteTasks;
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits)
    {
        // assign each split of splits to all workers
        Split eachSplit = splits.iterator().next();
        return nodeSelector.replicatedReadsComputeAssignments(eachSplit, remoteTasks.get(), bucketNodeMap);
    }

    @Override
    public void lockDownNodes()
    {}

    @Override
    public List<InternalNode> getActiveNodes()
    {
        return allNodes;
    }
}
