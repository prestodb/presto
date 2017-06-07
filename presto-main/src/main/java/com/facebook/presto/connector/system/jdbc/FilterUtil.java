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
package com.facebook.presto.connector.system.jdbc;

import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.predicate.AllExpression;
import com.facebook.presto.spi.predicate.AndExpression;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.NoneExpression;
import com.facebook.presto.spi.predicate.NotExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.TupleExpression;
import com.facebook.presto.spi.predicate.TupleExpressionVisitor;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;

import java.util.Optional;

final class FilterUtil
{
    private FilterUtil() {}

    public static Optional<String> stringFilter(TupleExpression<Integer> constraint, int index)
    {
        return constraint.accept(new Extractor(index), null);
    }

    private static class Extractor
            implements TupleExpressionVisitor<Optional<String>, Void, Integer>
    {
        int index;

        public Extractor(int index)
        {
            this.index = index;
        }

        @Override
        public Optional<String> visitDomainExpression(DomainExpression<Integer> expression, Void context)
        {
            if (expression.getColumn().equals(index)) {
                if (expression.getDomain() != null && expression.getDomain().isSingleValue()) {
                    Object value = expression.getDomain().getSingleValue();
                    if (value instanceof Slice) {
                        return Optional.of(((Slice) value).toStringUtf8());
                    }
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<String> visitAndExpression(AndExpression<Integer> expression, Void context)
        {
            Optional<String> left = expression.getLeftExpression().accept(this, context);
            Optional<String> right = expression.getRightExpression().accept(this, context);
            if (left.isPresent()) {
                return left;
            }
            else if (right.isPresent()) {
                return right;
            }
            else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<String> visitOrExpression(OrExpression<Integer> expression, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> visitNotExpression(NotExpression<Integer> expression, Void context)
        {
            return expression.getExpression().accept(this, context);
        }

        @Override
        public Optional<String> visitAllExpression(AllExpression<Integer> expression, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> visitNoneExpression(NoneExpression<Integer> expression, Void context)
        {
            return Optional.empty();
        }
    }

    public static QualifiedTablePrefix tablePrefix(String catalog, Optional<String> schema, Optional<String> table)
    {
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalog);
        if (schema.isPresent()) {
            prefix = new QualifiedTablePrefix(catalog, schema.get());
            if (table.isPresent()) {
                prefix = new QualifiedTablePrefix(catalog, schema.get(), table.get());
            }
        }
        return prefix;
    }

    public static <T> Iterable<T> filter(Iterable<T> items, Optional<T> filter)
    {
        if (!filter.isPresent()) {
            return items;
        }
        return Iterables.filter(items, Predicates.equalTo(filter.get()));
    }

    public static <T> boolean emptyOrEquals(Optional<T> value, T other)
    {
        return !value.isPresent() || value.get().equals(other);
    }
}
