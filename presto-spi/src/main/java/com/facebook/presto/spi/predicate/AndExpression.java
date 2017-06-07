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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class AndExpression<C>
        extends TupleExpression<C>
{
    TupleExpression<C> leftExpression;
    TupleExpression<C> rightExpression;

    @JsonCreator
    public AndExpression(
            @JsonProperty("leftExpression") TupleExpression leftExpression,
            @JsonProperty("rightExpression") TupleExpression rightExpression)
    {
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
    }

    @JsonProperty
    public TupleExpression<C> getLeftExpression()
    {
        return leftExpression;
    }

    @JsonProperty
    public TupleExpression<C> getRightExpression()
    {
        return rightExpression;
    }

    @Override
    public Map<C, NullableValue> extractFixedValues()
    {
        Map<C, NullableValue> map = new HashMap<>();
        map.putAll(leftExpression.extractFixedValues());
        map.putAll(rightExpression.extractFixedValues());
        return map;
    }

    @Override
    public List<C> getColumnDomains()
    {
        List<C> columnList = new ArrayList<C>();
        columnList.addAll(leftExpression.getColumnDomains());
        columnList.addAll(rightExpression.getColumnDomains());
        return columnList;
    }

    @Override
    public <R, T> R accept(TupleExpressionVisitor<R, T, C> visitor, T context)
    {
        return visitor.visitAndExpression(this, context);
    }

    @Override
    public TupleExpression transform(Function function)
    {
        return new AndExpression<C>(leftExpression.transform(function), rightExpression.transform(function));
    }

    @Override
    public String getName()
    {
        return "and";
    }

    @Override
    public boolean isNone()
    {
        return leftExpression.isNone() || rightExpression.isNone();
    }

    @Override
    public boolean isAll()
    {
        return leftExpression.isAll() && rightExpression.isAll();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(leftExpression, rightExpression);
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
        AndExpression other = (AndExpression) obj;
        return Objects.equals(this.leftExpression, other.leftExpression) &&
                Objects.equals(this.rightExpression, other.rightExpression);
    }

    @Override
    public String toString()
    {
        return '(' + leftExpression.toString() + ") AND (" + rightExpression.toString() + ")";
    }
}
