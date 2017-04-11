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
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DynamicSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    private final NodeSelector nodeSelector;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;

    public DynamicSplitPlacementPolicy(NodeSelector nodeSelector, Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits)
    {
        return nodeSelector.computeAssignments(splits, remoteTasks.get());
    }

    @Override
    public void lockDownNodes()
    {
        nodeSelector.lockDownNodes();
    }

    @Override
    public List<Node> allNodes()
    {
        return nodeSelector.allNodes();
    }
}
