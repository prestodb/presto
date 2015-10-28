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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.spi.Node;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.COORDINATOR_ONLY;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.FIXED;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.SINGLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;

    @Inject
    public NodePartitioningManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle)
    {
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (!(partitioningHandle instanceof SystemPartitioningHandle)) {
            throw new IllegalArgumentException("Unsupported partitioning handle " + partitioningHandle.getClass().getName());
        }

        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
        PlanDistribution planDistribution = ((SystemPartitioningHandle) partitioningHandle).getPlanDistribution();

        List<Node> nodes;
        if (planDistribution == COORDINATOR_ONLY) {
            nodes = ImmutableList.of(nodeSelector.selectCurrentNode());
        }
        else if (planDistribution == SINGLE) {
            nodes = nodeSelector.selectRandomNodes(1);
        }
        else if (planDistribution == FIXED) {
            nodes = nodeSelector.selectRandomNodes(getHashPartitionCount(session));
        }
        else {
            throw new IllegalArgumentException("Unsupported plan distribution " + planDistribution);
        }

        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

        ImmutableMap.Builder<Integer, Node> partitionToNode = ImmutableMap.builder();
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            partitionToNode.put(i, node);
        }
        return new NodePartitionMap(partitionToNode.build());
    }
}
