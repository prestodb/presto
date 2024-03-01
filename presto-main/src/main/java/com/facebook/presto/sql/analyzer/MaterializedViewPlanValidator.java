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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Unnest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

// TODO: Add more cases https://github.com/prestodb/presto/issues/16032
public class MaterializedViewPlanValidator
        extends DefaultTraversalVisitor<Void, MaterializedViewPlanValidator.MaterializedViewPlanValidatorContext>
{
    protected MaterializedViewPlanValidator()
    {}

    public static void validate(Query viewQuery)
    {
        new MaterializedViewPlanValidator().process(viewQuery, new MaterializedViewPlanValidatorContext());
    }

    @Override
    protected Void visitTable(Table node, MaterializedViewPlanValidatorContext context)
    {
        // Materialized View Definition does not support have multiple instances of same table. We have this assumption throughout our codebase as we use it
        // for keys in several maps. For e.g. Partition mapping logic would need to be rewritten by considering partitions from each instance
        // of base table separately. We will need to use (table name + node location) as an identifier in all such places. For now, we just
        // forbid it.
        if (!context.addTable(node)) {
            throw new SemanticException(NOT_SUPPORTED, node, "Materialized View definition does not support multiple instances of same table");
        }

        return super.visitTable(node, context);
    }

    @Override
    protected Void visitQuery(Query node, MaterializedViewPlanValidatorContext context)
    {
        if (node.getLimit().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "LIMIT clause in materialized view is not supported.");
        }
        return super.visitQuery(node, context);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, MaterializedViewPlanValidatorContext context)
    {
        if (node.getLimit().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "LIMIT clause in materialized view is not supported.");
        }
        return super.visitQuerySpecification(node, context);
    }

    @Override
    protected Void visitJoin(Join node, MaterializedViewPlanValidatorContext context)
    {
        context.pushJoinNode(node);

        JoinCriteria joinCriteria;
        switch (node.getType()) {
            case INNER:
                if (!node.getCriteria().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Inner join with no criteria is not supported for materialized view.");
                }

                joinCriteria = node.getCriteria().get();
                if (!(joinCriteria instanceof JoinOn)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Only join-on is supported for materialized view.");
                }

                process(node.getLeft(), context);
                process(node.getRight(), context);

                context.setWithinJoinOn(true);
                process(((JoinOn) joinCriteria).getExpression(), context);
                context.setWithinJoinOn(false);

                break;
            case CROSS:
                if (!(node.getRight() instanceof AliasedRelation)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Cross join is supported only with unnest for materialized view.");
                }
                AliasedRelation right = (AliasedRelation) node.getRight();
                if (!(right.getRelation() instanceof Unnest)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Cross join is supported only with unnest for materialized view.");
                }

                process(node.getLeft(), context);

                break;

            case LEFT:
                if (!node.getCriteria().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Outer join with no criteria is not supported for materialized view.");
                }

                joinCriteria = node.getCriteria().get();
                if (!(joinCriteria instanceof JoinOn)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Only join-on is supported for materialized view.");
                }

                process(node.getLeft(), context);

                boolean wasWithinOuterJoin = context.isWithinOuterJoin();
                context.setWithinOuterJoin(true);
                process(node.getRight(), context);
                // withinOuterJoin denotes if we are within an outer side of a join. Because it can be nested, replace it with its older value.
                // So we set it to false, only when we leave the topmost outer join.
                context.setWithinOuterJoin(wasWithinOuterJoin);

                context.setWithinJoinOn(true);
                process(((JoinOn) joinCriteria).getExpression(), context);
                context.setWithinJoinOn(false);

                break;

            default:
                throw new SemanticException(NOT_SUPPORTED, node, "Only inner join, left join and cross join unnested are supported for materialized view.");
        }

        context.popJoinNode();
        return null;
    }

    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, MaterializedViewPlanValidatorContext context)
    {
        if (context.isWithinJoinOn()) {
            if (!node.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only AND operator is supported for join criteria for materialized view.");
            }
        }

        return super.visitLogicalBinaryExpression(node, context);
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, MaterializedViewPlanValidatorContext context)
    {
        if (context.isWithinJoinOn()) {
            if (!node.getOperator().equals(ComparisonExpression.Operator.EQUAL)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only EQUAL join is supported for materialized view.");
            }
        }

        return super.visitComparisonExpression(node, context);
    }

    @Override
    protected Void visitSubqueryExpression(SubqueryExpression node, MaterializedViewPlanValidatorContext context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "Subqueries are not supported for materialized view.");
    }

    @Override
    protected Void visitOrderBy(OrderBy node, MaterializedViewPlanValidatorContext context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "OrderBy are not supported for materialized view.");
    }

    public static final class MaterializedViewPlanValidatorContext
    {
        private boolean isWithinJoinOn;
        private final LinkedList<Join> joinNodeStack;
        private boolean isWithinOuterJoin;
        private final HashSet<Table> tables;

        public MaterializedViewPlanValidatorContext()
        {
            isWithinJoinOn = false;
            joinNodeStack = new LinkedList<>();
            isWithinOuterJoin = false;
            tables = new HashSet<>();
        }

        public boolean isWithinJoinOn()
        {
            return isWithinJoinOn;
        }

        public void setWithinJoinOn(boolean withinJoinOn)
        {
            isWithinJoinOn = withinJoinOn;
        }

        public boolean isWithinOuterJoin()
        {
            return isWithinOuterJoin;
        }

        public void setWithinOuterJoin(boolean withinOuterJoin)
        {
            isWithinOuterJoin = withinOuterJoin;
        }

        public void pushJoinNode(Join join)
        {
            joinNodeStack.push(join);
        }

        public Join popJoinNode()
        {
            return joinNodeStack.pop();
        }

        public Join getTopJoinNode()
        {
            return joinNodeStack.getFirst();
        }

        public List<Join> getJoinNodes()
        {
            return ImmutableList.copyOf(joinNodeStack);
        }

        public boolean addTable(Table table)
        {
            return tables.add(table);
        }

        public Set<Table> getTables()
        {
            return ImmutableSet.copyOf(tables);
        }
    }
}
