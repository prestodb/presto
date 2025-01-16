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
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Query;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.getMaxLeafNodesInPlan;
import static com.facebook.presto.SystemSessionProperties.isLeafNodeLimitEnabled;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_PLAN_NODE_LIMIT;
import static java.lang.String.format;

public class SqlPlannerContext
{
    // Record the current number of leaf nodes (table scan and value nodes) in query plan during planning
    private int leafNodesInLogicalPlan;
    private final SqlToRowExpressionTranslator.Context translatorContext;

    private final CteInfo cteInfo;

    public SqlPlannerContext(int leafNodesInLogicalPlan)
    {
        this.leafNodesInLogicalPlan = leafNodesInLogicalPlan;
        this.translatorContext = new SqlToRowExpressionTranslator.Context();
        this.cteInfo = new CteInfo();
    }

    public CteInfo getCteInfo()
    {
        return cteInfo;
    }

    public SqlToRowExpressionTranslator.Context getTranslatorContext()
    {
        return translatorContext;
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

    public class CteInfo
    {
        @VisibleForTesting
        public static final String delimiter = "_*%$_";
        // never decreases
        private int prefix;

        // Map a cte Query to a unique ID, which will be used in CTE reference node to identify the same CTE
        private final Map<NodeRef<Query>, String> cteQueryUniqueIdMap = new HashMap<>();

        public String normalize(NodeRef<Query> queryNodeRef, String cteName)
        {
            if (cteQueryUniqueIdMap.containsKey(queryNodeRef)) {
                return cteQueryUniqueIdMap.get(queryNodeRef) + delimiter + cteName;
            }
            String identityString = String.valueOf(prefix++);
            cteQueryUniqueIdMap.put(queryNodeRef, identityString);
            return identityString + delimiter + cteName;
        }
    }
}
