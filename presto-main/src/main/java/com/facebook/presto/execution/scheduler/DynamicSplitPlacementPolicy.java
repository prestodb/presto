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
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSplitAssigner;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DynamicSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    private final NodeSplitAssigner nodeSplitAssigner;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;

    public DynamicSplitPlacementPolicy(NodeSplitAssigner nodeSplitAssigner, Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSplitAssigner = requireNonNull(nodeSplitAssigner, "nodeSelector is null");
        this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits)
    {
        return nodeSplitAssigner.computeAssignments(splits, remoteTasks.get());
    }

    @Override
    public void lockDownNodes()
    {
        nodeSplitAssigner.lockDownNodes();
    }

    @Override
    public List<InternalNode> getActiveNodes()
    {
        return nodeSplitAssigner.getActiveNodes();
    }
}
