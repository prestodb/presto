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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QuantifiedComparisonExpression
        extends Expression
{
    public enum Quantifier
    {
        ALL,
        ANY,
        SOME,
    }

    private final ComparisonExpression.Operator operator;
    private final Quantifier quantifier;
    private final Expression value;
    private final Expression subquery;

    public QuantifiedComparisonExpression(ComparisonExpression.Operator operator, Quantifier quantifier, Expression value, Expression subquery)
    {
        this(Optional.empty(), operator, quantifier, value, subquery);
    }

    public QuantifiedComparisonExpression(NodeLocation location, ComparisonExpression.Operator operator, Quantifier quantifier, Expression value, Expression subquery)
    {
        this(Optional.of(location), operator, quantifier, value, subquery);
    }

    private QuantifiedComparisonExpression(Optional<NodeLocation> location, ComparisonExpression.Operator operator, Quantifier quantifier, Expression value, Expression subquery)
    {
        super(location);
        this.operator = requireNonNull(operator, "comparisonType is null");
        this.quantifier = requireNonNull(quantifier, "quantifier is null");
        this.value = requireNonNull(value, "value is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
    }

    public ComparisonExpression.Operator getOperator()
    {
        return operator;
    }

    public Quantifier getQuantifier()
    {
        return quantifier;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getSubquery()
    {
        return subquery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQuantifiedComparisonExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value, subquery);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QuantifiedComparisonExpression that = (QuantifiedComparisonExpression) o;
        return operator == that.operator &&
                quantifier == that.quantifier &&
                Objects.equals(value, that.value) &&
                Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, quantifier, value, subquery);
    }
}
