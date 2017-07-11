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
import com.facebook.presto.sql.planner.NodePartitionMap;

import java.util.List;
import java.util.Set;

public interface NodeSelector
{
    void lockDownNodes();

    List<Node> allNodes();

    Node selectCurrentNode();

    List<Node> selectRandomNodes(int limit);

    /**
     * Identifies the nodes for running the specified splits.
     *
     * @param splits the splits that need to be assigned to nodes
     * @return a multimap from node to splits only for splits for which we could identify a node to schedule on.
     * If we cannot find an assignment for a split, it is not included in the map. Also returns a future indicating when
     * to reattempt scheduling of this batch of splits, if some of them could not be scheduled.
     */
    SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks);

    /**
     * Identifies the nodes for running the specified splits based on a precomputed fixed partitioning.
     *
     * @param splits the splits that need to be assigned to nodes
     * @return a multimap from node to splits only for splits for which we could identify a node with free space.
     * If we cannot find an assignment for a split, it is not included in the map. Also returns a future indicating when
     * to reattempt scheduling of this batch of splits, if some of them could not be scheduled.
     */
    SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, NodePartitionMap partitioning);
}
