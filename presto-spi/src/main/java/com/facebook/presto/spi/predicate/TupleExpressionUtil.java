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
package com.facebook.presto.spi.predicate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TupleExpressionUtil
{
    private TupleExpressionUtil()
    {}

    public static <T> Optional<Map<T, NullableValue>> extractFixedValues(TupleExpression<T> tupleExpression)
    {
        Map<T, NullableValue> map = tupleExpression.accept(new FixedValuesExtractor<T>(), null);
        if (map.isEmpty()) {
            return Optional.empty();
        }

        else {
            return Optional.of(map);
        }
    }

    private static class FixedValuesExtractor<T>
            implements TupleExpressionVisitor<Map<T, NullableValue>, Void, T>
    {
        @Override
        public Map<T, NullableValue> visitDomainExpression(DomainExpression<T> expression, Void context)
        {
            Map<T, NullableValue> returnMap = new HashMap<>();
            Domain domain = expression.getDomain();
            if (domain.isNullableSingleValue()) {
                returnMap.put(expression.getColumn(), new NullableValue(domain.getType(), domain.getNullableSingleValue()));
            }
            return returnMap;
        }

        @Override
        public Map<T, NullableValue> visitAndExpression(AndExpression<T> expression, Void context)
        {
            Map<T, NullableValue> returnMap = new HashMap<>();
            returnMap.putAll(expression.getLeftExpression().accept(this, context));
            returnMap.putAll(expression.getRightExpression().accept(this, context));
            return returnMap;
        }

        @Override
        public Map<T, NullableValue> visitOrExpression(OrExpression<T> expression, Void context)
        {
            return new HashMap<>();
        }

        @Override
        public Map<T, NullableValue> visitNotExpression(NotExpression<T> expression, Void context)
        {
            return expression.getExpression().accept(this, context);
        }

        @Override
        public Map<T, NullableValue> visitAllExpression(AllExpression<T> expression, Void context)
        {
            return new HashMap<>();
        }

        @Override
        public Map<T, NullableValue> visitNoneExpression(NoneExpression<T> expression, Void context)
        {
            return new HashMap<>();
        }
    }

    public static <T> TupleDomain<T> toTupleDomain(TupleExpression<T> expression)
    {

        Optional<Map<T, Domain>> domainMap = expression.accept(new TupleDomainTranslator<T>(), null);
        if (domainMap.isPresent()) {
            return TupleDomain.withColumnDomains(domainMap.get());
        }
        return TupleDomain.none();
    }

    private static class TupleDomainTranslator<T>
            implements TupleExpressionVisitor<Optional<Map<T, Domain>>, Void, T>
    {
        @Override
        public Optional<Map<T, Domain>> visitDomainExpression(DomainExpression<T> expression, Void context)
        {
            Map<T, Domain> map = new HashMap<>();
            map.put(expression.getColumn(), expression.getDomain());
            return Optional.of(map);
        }

        @Override
        public Optional<Map<T, Domain>> visitAndExpression(AndExpression<T> expression, Void context)
        {
            Map<T, Domain> returnMap = new HashMap<>();
            Optional<Map<T, Domain>> leftDomain = expression.getLeftExpression().accept(this, context);
            Optional<Map<T, Domain>> rightDomain = expression.getRightExpression().accept(this, context);
            if (!leftDomain.isPresent() || !rightDomain.isPresent()) {
                return Optional.empty();
            }
            returnMap.putAll(expression.getLeftExpression().accept(this, context).get());
            returnMap.putAll(expression.getRightExpression().accept(this, context).get());
            return Optional.of(returnMap);
        }

        @Override
        public Optional<Map<T, Domain>> visitOrExpression(OrExpression<T> expression, Void context)
        {
            return Optional.of(Collections.<T, Domain>emptyMap());
        }

        @Override
        public Optional<Map<T, Domain>> visitNotExpression(NotExpression<T> expression, Void context)
        {
            return expression.getExpression().accept(this, context);
        }

        @Override
        public Optional<Map<T, Domain>> visitAllExpression(AllExpression<T> expression, Void context)
        {
            return Optional.of(Collections.<T, Domain>emptyMap());
        }

        @Override
        public Optional<Map<T, Domain>> visitNoneExpression(NoneExpression<T> expression, Void context)
        {
            return Optional.empty();
        }
    }

    public static <T> TupleExpression<T> getAndExpression(TupleExpression<T> leftExpresison, TupleExpression<T> rightExpression)
    {
        if (leftExpresison instanceof NoneExpression || rightExpression instanceof NoneExpression) {
            return new NoneExpression<T>();
        }
        if (leftExpresison instanceof AllExpression && !(rightExpression instanceof NoneExpression)) {
            return rightExpression;
        }
        else if (rightExpression instanceof AllExpression && !(leftExpresison instanceof NoneExpression)) {
            return leftExpresison;
        }
        return new AndExpression<T>(leftExpresison, rightExpression);
    }

    public static <T> TupleExpression<T> getTupleExpression(TupleDomain<T> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return new AllExpression<T>();
        }
        else if (tupleDomain.isNone()) {
            return new NoneExpression<T>();
        }
        TupleExpression<T> baseExpression = new AllExpression<T>();
        for (Map.Entry<T, Domain> domain : tupleDomain.getDomains().get().entrySet()) {
            baseExpression = getAndExpression(baseExpression, new DomainExpression<T>(domain.getKey(), domain.getValue()));
        }
        return baseExpression;
    }
}

