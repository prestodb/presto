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
import com.facebook.presto.sql.tree.Query;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static com.facebook.presto.SystemSessionProperties.getMaxLeafNodesInPlan;
import static com.facebook.presto.SystemSessionProperties.isLeafNodeLimitEnabled;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_PLAN_NODE_LIMIT;
import static java.lang.String.format;

public class SqlPlannerContext
{
    // Record the current number of leaf nodes (table scan and value nodes) in query plan during planning
    private int leafNodesInLogicalPlan;
    private final SqlToRowExpressionTranslator.Context translatorContext;

    private final NestedCteStack nestedCteStack;

    public SqlPlannerContext(int leafNodesInLogicalPlan)
    {
        this.leafNodesInLogicalPlan = leafNodesInLogicalPlan;
        this.translatorContext = new SqlToRowExpressionTranslator.Context();
        this.nestedCteStack = new NestedCteStack();
    }

    public NestedCteStack getNestedCteStack()
    {
        return nestedCteStack;
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

    public class NestedCteStack
    {
        @VisibleForTesting
        public static final String delimiter = "_*%$_";
        private final Stack<String> cteStack;
        private final Map<String, String> rawCtePathMap;

        public NestedCteStack()
        {
            this.cteStack = new Stack<>();
            this.rawCtePathMap = new HashMap<>();
        }

        public void push(String cteName, Query query)
        {
            this.cteStack.push(cteName);
            if (query.getWith().isPresent()) {
                // All ctes defined in this context should have their paths updated
                query.getWith().get().getQueries().forEach(with -> this.addNestedCte(with.getName().toString()));
            }
        }

        public void pop(Query query)
        {
            this.cteStack.pop();
            if (query.getWith().isPresent()) {
                query.getWith().get().getQueries().forEach(with -> this.removeNestedCte(with.getName().toString()));
            }
        }

        public String getRawPath(String cteName)
        {
            if (!this.rawCtePathMap.containsKey(cteName)) {
                return cteName;
            }
            return this.rawCtePathMap.get(cteName);
        }

        private void addNestedCte(String cteName)
        {
            this.rawCtePathMap.put(cteName, getCurrentRelativeCtePath() + delimiter + cteName);
        }

        private void removeNestedCte(String cteName)
        {
            this.rawCtePathMap.remove(cteName);
        }

        public String getCurrentRelativeCtePath()
        {
            return String.join(delimiter, cteStack);
        }
    }
}
