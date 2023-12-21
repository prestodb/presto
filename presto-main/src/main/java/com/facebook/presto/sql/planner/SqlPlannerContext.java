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
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Table;
import com.google.common.annotations.VisibleForTesting;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

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
        private int currentQueryScopeId;

        // Maps a set of Query objects, including the parent query statement and all its referenced statements,
        // to a unique scope identifier. Each set of related queries shares the same scope.
        Map<TreeSet<Query>, String> queryNodeScopeIdMap = new HashMap<>();

        public String normalize(Analysis analysis, Query query, String cteName)
        {
            QueryReferenceCollectorContext context = new QueryReferenceCollectorContext();
            context.getReferencedQuerySet().add(query);
            query.accept(new QueryReferenceCollector(analysis), context);
            TreeSet<Query> normalizedKey = context.getReferencedQuerySet();
            if (!queryNodeScopeIdMap.containsKey(normalizedKey)) {
                queryNodeScopeIdMap.put(normalizedKey, String.valueOf(currentQueryScopeId++));
            }
            return queryNodeScopeIdMap.get(normalizedKey) + delimiter + cteName;
        }

        private class QueryReferenceCollector
                extends DefaultTraversalVisitor<Void, QueryReferenceCollectorContext>
        {
            private final Analysis analysis;

            public QueryReferenceCollector(Analysis analysis)
            {
                this.analysis = analysis;
            }

            @Override
            protected Void visitTable(Table node, QueryReferenceCollectorContext context)
            {
                Analysis.NamedQuery namedQuery = analysis.getNamedQuery(node);
                if (namedQuery != null) {
                    context.addQuery(namedQuery.getQuery());
                    process(namedQuery.getQuery(), context);
                }
                return null;
            }
        }

        private class QueryReferenceCollectorContext
        {
            private final TreeSet<Query> referencedQuerySet;

            public QueryReferenceCollectorContext()
            {
                this.referencedQuerySet = new TreeSet<>(Comparator.comparingInt(Query::hashCode));
            }

            public void addQuery(Query ref)
            {
                this.referencedQuerySet.add(ref);
            }

            public TreeSet<Query> getReferencedQuerySet()
            {
                return referencedQuerySet;
            }
        }
    }
}
