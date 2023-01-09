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
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.SystemSessionProperties.getMaxLeafNodesInPlan;
import static com.facebook.presto.SystemSessionProperties.isLeafNodeLimitEnabled;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_PLAN_NODE_LIMIT;
import static java.lang.String.format;

public class SqlPlannerContext
{
    // Record the current number of leaf nodes (table scan and value nodes) in query plan during planning
    private int leafNodesInLogicalPlan;

    public SqlPlannerContext(int leafNodesInLogicalPlan)
    {
        this.leafNodesInLogicalPlan = leafNodesInLogicalPlan;
    }

    public void incrementLeafNodes(Session session)
    {
        leafNodesInLogicalPlan += 1;
        if (isLeafNodeLimitEnabled(session)) {
            if (leafNodesInLogicalPlan > getMaxLeafNodesInPlan(session)) {
                throw new PrestoException(EXCEEDED_PLAN_NODE_LIMIT, format("Number of leaf nodes in logical plan exceeds threshold %s set in max_leaf_nodes_in_plan",
                        getMaxLeafNodesInPlan(session)));
            }
        }
    }
}
