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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.AllExpression;
import com.facebook.presto.spi.predicate.AndExpression;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.NoneExpression;
import com.facebook.presto.spi.predicate.NotExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.TupleExpression;
import com.facebook.presto.spi.predicate.TupleExpressionVisitor;
import com.facebook.presto.spi.predicate.ValueSet;

import java.util.Optional;

public class TupleExpressionCompactor
        implements TupleExpressionVisitor<TupleExpression, Void, ColumnHandle>
{
    private final int threshold;

    public TupleExpressionCompactor(int threshold)
    {
        this.threshold = threshold;
    }

    @Override
    public TupleExpression visitDomainExpression(DomainExpression<ColumnHandle> expression, Void context)
    {
        ValueSet values = expression.getDomain().getValues();
        ValueSet compactValueSet = values.getValuesProcessor().<Optional<ValueSet>>transform(
                ranges -> ranges.getRangeCount() > threshold ? Optional.of(ValueSet.ofRanges(ranges.getSpan())) : Optional.empty(),
                discreteValues -> discreteValues.getValues().size() > threshold ? Optional.of(ValueSet.all(values.getType())) : Optional.empty(),
                allOrNone -> Optional.empty())
                .orElse(values);
        return new DomainExpression(expression.getColumn(), Domain.create(compactValueSet, expression.getDomain().isNullAllowed()));
    }

    @Override
    public TupleExpression visitAndExpression(AndExpression<ColumnHandle> expression, Void context)
    {
        return new OrExpression(expression.getLeftExpression().accept(this, context),
                expression.getRightExpression().accept(this, context));
    }

    @Override
    public TupleExpression visitOrExpression(OrExpression<ColumnHandle> expression, Void context)
    {
        return new AndExpression(expression.getLeftExpression().accept(this, context),
                expression.getRightExpression().accept(this, context));
    }

    @Override
    public TupleExpression visitNotExpression(NotExpression<ColumnHandle> expression, Void context)
    {
        return new NotExpression(expression.getExpression().accept(this, context));
    }

    @Override
    public TupleExpression visitAllExpression(AllExpression<ColumnHandle> expression, Void context)
    {
        return expression;
    }

    @Override
    public TupleExpression visitNoneExpression(NoneExpression<ColumnHandle> expression, Void context)
    {
        return expression;
    }
}
