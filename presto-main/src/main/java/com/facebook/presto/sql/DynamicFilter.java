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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.DynamicFilterUtils.isDynamicFilter;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicFilter
{
    private final Expression sourceExpression;
    private final String tupleDomainSourceId;
    private final String tupleDomainName;
    private final ComparisonExpressionType comparisonType;

    public static Optional<DynamicFilter> getDynamicFilterOptional(Expression expression)
    {
        if (!isDynamicFilter(expression)) {
            return Optional.empty();
        }
        return Optional.of(from(expression));
    }

    public static DynamicFilter from(Expression expression)
    {
        checkArgument(expression instanceof ComparisonExpression, "Unexpected expression: %s", expression);
        ComparisonExpression comparison = (ComparisonExpression) expression;

        Expression sourceExpression = null;
        DeferredSymbolReference deferredReference = null;

        checkState(comparison.getLeft() instanceof DeferredSymbolReference ^ comparison.getRight() instanceof DeferredSymbolReference, "Exactly one deferred symbol per expression is required");
        if (comparison.getLeft() instanceof DeferredSymbolReference) {
            sourceExpression = comparison.getRight();
            deferredReference = (DeferredSymbolReference) comparison.getLeft();
        }
        else if (comparison.getRight() instanceof DeferredSymbolReference) {
            sourceExpression = comparison.getLeft();
            deferredReference = (DeferredSymbolReference) comparison.getRight();
        }

        return new DynamicFilter(
                sourceExpression,
                deferredReference.getSourceId(),
                deferredReference.getSymbol(),
                comparison.getType());
    }

    public DynamicFilter(Expression sourceExpression, String tupleDomainSourceId, String tupleDomainName, ComparisonExpressionType comparisonType)
    {
        this.sourceExpression = requireNonNull(sourceExpression, "symbol is null");
        this.tupleDomainSourceId = requireNonNull(tupleDomainSourceId, "tupleDomainSourceId is null");
        this.tupleDomainName = requireNonNull(tupleDomainName, "tupleDomainName is null");
        this.comparisonType = requireNonNull(comparisonType, "comparisonType is null");
    }

    public Expression getSourceExpression()
    {
        return sourceExpression;
    }

    public String getTupleDomainSourceId()
    {
        return tupleDomainSourceId;
    }

    public String getTupleDomainName()
    {
        return tupleDomainName;
    }

    public ComparisonExpressionType getComparisonType()
    {
        return comparisonType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sourceExpression", sourceExpression)
                .add("tupleDomainSourceId", tupleDomainSourceId)
                .add("tupleDomainName", tupleDomainName)
                .add("comparisonType", comparisonType)
                .toString();
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
        DynamicFilter that = (DynamicFilter) o;
        return Objects.equals(sourceExpression, that.sourceExpression) &&
                Objects.equals(tupleDomainSourceId, that.tupleDomainSourceId) &&
                Objects.equals(tupleDomainName, that.tupleDomainName) &&
                comparisonType == that.comparisonType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceExpression, tupleDomainSourceId, tupleDomainName, comparisonType);
    }
}
