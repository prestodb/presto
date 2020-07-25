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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;

public final class JoinNodeUtils
{
    private JoinNodeUtils() {}

    public static ComparisonExpression toExpression(EquiJoinClause clause)
    {
        return new ComparisonExpression(EQUAL, new SymbolReference(clause.getLeft().getName()), new SymbolReference(clause.getRight().getName()));
    }

    public static RowExpression toRowExpression(EquiJoinClause clause, FunctionResolution functionResolution)
    {
        return call(
                OperatorType.EQUAL.name(),
                functionResolution.comparisonFunction(OperatorType.EQUAL, clause.getLeft().getType(), clause.getRight().getType()),
                BOOLEAN,
                ImmutableList.of(clause.getLeft(), clause.getRight()));
    }

    public static JoinNode.Type typeConvert(Join.Type joinType)
    {
        // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
        switch (joinType) {
            case CROSS:
            case IMPLICIT:
            case INNER:
                return INNER;
            case LEFT:
                return LEFT;
            case RIGHT:
                return RIGHT;
            case FULL:
                return FULL;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }
}
