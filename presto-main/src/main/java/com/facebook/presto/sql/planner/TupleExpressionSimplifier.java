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

import com.facebook.presto.spi.predicate.AllExpression;
import com.facebook.presto.spi.predicate.AndExpression;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.NoneExpression;
import com.facebook.presto.spi.predicate.NotExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.TupleExpression;
import com.facebook.presto.spi.predicate.TupleExpressionVisitor;

public class TupleExpressionSimplifier<C>
        implements TupleExpressionVisitor<TupleExpression, Void, C>
{
    @Override
    public TupleExpression visitDomainExpression(DomainExpression<C> expression, Void context)
    {
        return new DomainExpression(expression.getColumn(), DomainUtils.simplifyDomain(expression.getDomain()));
    }

    @Override
    public TupleExpression visitAndExpression(AndExpression<C> expression, Void context)
    {
        TupleExpression leftExpression = expression.getLeftExpression().accept(this, context);
        TupleExpression rightExpression = expression.getRightExpression().accept(this, context);
        return new AndExpression(leftExpression, rightExpression);
    }

    @Override
    public TupleExpression visitOrExpression(OrExpression<C> expression, Void context)
    {
        TupleExpression leftExpression = expression.getLeftExpression().accept(this, context);
        TupleExpression rightExpression = expression.getRightExpression().accept(this, context);
        return new OrExpression(leftExpression, rightExpression);
    }

    @Override
    public TupleExpression visitNotExpression(NotExpression<C> expression, Void context)
    {
        return new NotExpression(expression.accept(this, context));
    }

    @Override
    public TupleExpression visitAllExpression(AllExpression<C> expression, Void context)
    {
        return expression;
    }

    @Override
    public TupleExpression visitNoneExpression(NoneExpression<C> expression, Void context)
    {
        return expression;
    }
}
