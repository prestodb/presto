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
package com.facebook.presto.spi.relation;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SourceLocation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * RowExpression equivalent of QuantifiedComparisonExpression.
 */
public final class QuantifiedComparisonRowExpression
        extends IntermediateFormRowExpression
{
    public enum Quantifier
    {
        ALL,
        ANY,
        SOME,
    }

    private final OperatorType operator;
    private final Quantifier quantifier;
    private final RowExpression value;
    private final RowExpression subquery;

    public QuantifiedComparisonRowExpression(Optional<SourceLocation> sourceLocation, OperatorType operator, Quantifier quantifier, RowExpression value, RowExpression subquery)
    {
        super(sourceLocation);
        this.operator = requireNonNull(operator, "operator is null");
        this.quantifier = requireNonNull(quantifier, "quantifier is null");
        this.value = requireNonNull(value, "value is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
    }

    public OperatorType getOperator()
    {
        return operator;
    }

    public Quantifier getQuantifier()
    {
        return quantifier;
    }

    public RowExpression getValue()
    {
        return value;
    }

    public RowExpression getSubquery()
    {
        return subquery;
    }

    @Override
    public Type getType()
    {
        return BOOLEAN;
    }

    @Override
    public List<RowExpression> getChildren()
    {
        return unmodifiableList(asList(value, subquery));
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

        QuantifiedComparisonRowExpression that = (QuantifiedComparisonRowExpression) o;
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

    @Override
    public String toString()
    {
        return format("%s %s %s (%s)", value, operator, quantifier, subquery);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntermediateFormRowExpression(this, context);
    }

    @Override
    public RowExpression canonicalize()
    {
        return this;
    }
}
