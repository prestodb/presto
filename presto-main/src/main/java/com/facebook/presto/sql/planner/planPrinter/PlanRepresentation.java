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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class PlanRepresentation
{
    private final PlanNode root;
    private final int level;

    private final Map<PlanNodeId, NodeRepresentation> nodeInfo = new HashMap<>();

    public PlanRepresentation(PlanNode root, int level)
    {
        this.root = root;
        this.level = level;
    }

    public int getLevel()
    {
        return level;
    }

    public NodeRepresentation getRoot()
    {
        return nodeInfo.get(root.getId());
    }

    public Optional<NodeRepresentation> getNode(PlanNodeId id)
    {
        return Optional.ofNullable(nodeInfo.get(id));
    }

    public void addNode(NodeRepresentation node)
    {
        nodeInfo.put(node.getId(), node);
    }
}
