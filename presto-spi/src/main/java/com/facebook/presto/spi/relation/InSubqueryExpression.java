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
 * RowExpression equivalent of InPredicate with subqueries.
 */
public class InSubqueryExpression
        extends IntermediateFormExpression
{
    private final VariableReferenceExpression value;
    private final VariableReferenceExpression subquery;

    public InSubqueryExpression(Optional<SourceLocation> sourceLocation, VariableReferenceExpression value, VariableReferenceExpression subquery)
    {
        super(sourceLocation);
        this.value = requireNonNull(value, "value is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
    }

    public VariableReferenceExpression getValue()
    {
        return value;
    }

    public VariableReferenceExpression getSubquery()
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InSubqueryExpression other = (InSubqueryExpression) obj;
        return Objects.equals(this.value, other.value) &&
                Objects.equals(this.subquery, other.subquery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, subquery);
    }

    @Override
    public String toString()
    {
        return format("%s IN %s", value, subquery);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntermediateFormExpression(this, context);
    }

    @Override
    public RowExpression canonicalize()
    {
        return getSourceLocation().isPresent() ? new InSubqueryExpression(Optional.empty(), value, subquery) : this;
    }
}
