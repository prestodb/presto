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
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Unnest;
import com.google.common.collect.ImmutableList;

import java.util.LinkedList;
import java.util.List;

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
    protected Void visitJoin(Join node, MaterializedViewPlanValidatorContext context)
    {
        context.pushJoinNode(node);

        if (context.getJoinNodes().size() > 1) {
            throw new SemanticException(NOT_SUPPORTED, node, "More than one join in materialized view is not supported yet.");
        }

        switch (node.getType()) {
            case INNER:
                if (!node.getCriteria().isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Inner join with no criteria is not supported for materialized view.");
                }

                JoinCriteria joinCriteria = node.getCriteria().get();
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

            default:
                throw new SemanticException(NOT_SUPPORTED, node, "Only inner join and cross join unnested are supported for materialized view.");
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

    public static final class MaterializedViewPlanValidatorContext
    {
        private boolean isWithinJoinOn;
        private final LinkedList<Join> joinNodeStack;

        public MaterializedViewPlanValidatorContext()
        {
            isWithinJoinOn = false;
            joinNodeStack = new LinkedList<>();
        }

        public boolean isWithinJoinOn()
        {
            return isWithinJoinOn;
        }

        public void setWithinJoinOn(boolean withinJoinOn)
        {
            isWithinJoinOn = withinJoinOn;
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
    }
}
