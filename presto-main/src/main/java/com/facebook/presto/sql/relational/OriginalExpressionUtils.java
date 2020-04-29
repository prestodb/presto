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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The following has a list of helper functions to transform between Expression and RowExpression.
 * Ideally, users should never do type checking or down casting for future convenience of refactoring.
 */
public final class OriginalExpressionUtils
{
    private OriginalExpressionUtils() {}

    /**
     * Create a new RowExpression
     */
    public static RowExpression castToRowExpression(Expression expression)
    {
        return new OriginalExpression(expression);
    }

    public static SymbolReference asSymbolReference(VariableReferenceExpression variable)
    {
        return new SymbolReference(variable.getName());
    }

    /**
     * Degrade to Expression
     */
    public static Expression castToExpression(RowExpression rowExpression)
    {
        checkArgument(isExpression(rowExpression));
        return ((OriginalExpression) rowExpression).getExpression();
    }

    /**
     * Check if the given {@param rowExpression} is an Expression.
     */
    public static boolean isExpression(RowExpression rowExpression)
    {
        return (rowExpression instanceof OriginalExpression);
    }

    /**
     * OriginalExpression is a RowExpression container holding an {@param Expression} object
     * that cannot be translated directly while constructing PlanNode.
     * Typical examples are cases with `WITH` clauses or alias that the type or arguments information
     * are not directly available given the substrees are not formed yet.
     * OriginalExpression should not exist after optimization or serialized over the wire.
     * All OriginalExpression should be translated to other corresponding RowExpression objects ultimately.
     */
    private static final class OriginalExpression
            extends RowExpression
    {
        private final Expression expression;

        OriginalExpression(Expression expression)
        {
            this.expression = requireNonNull(expression, "expression is null");
        }

        public Expression getExpression()
        {
            return expression;
        }

        @Override
        public Type getType()
        {
            throw new UnsupportedOperationException("OriginalExpression does not have a type");
        }

        @Override
        public String toString()
        {
            return expression.toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OriginalExpression other = (OriginalExpression) obj;
            return Objects.equals(this.expression, other.expression);
        }

        @Override
        public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
        {
            throw new UnsupportedOperationException("OriginalExpression cannot appear in a RowExpression tree");
        }
    }
}
