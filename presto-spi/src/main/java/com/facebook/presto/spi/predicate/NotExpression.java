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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class NotExpression<C>
        extends TupleExpression<C>
{
    TupleExpression expression;

    @JsonCreator
    public NotExpression(@JsonProperty("expression") TupleExpression expression)
    {
        this.expression = expression;
    }

    @JsonProperty
    public TupleExpression getExpression()
    {
        return this.expression;
    }

    @Override
    public Map<C, NullableValue> extractFixedValues()
    {
        return expression.extractFixedValues();
    }

    @Override
    public List<C> getColumnDomains()
    {
        return expression.getColumnDomains();
    }

    @Override
    public TupleExpression transform(Function function)
    {
        return expression.transform(function);
    }

    @Override
    public <R, T> R accept(TupleExpressionVisitor<R, T> visitor, T context)
    {
        return visitor.visitNotExpression(this, context);
    }

    @Override
    public String getName()
    {
        return "not";
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(expression);
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
        NotExpression other = (NotExpression) obj;
        return Objects.equals(this.expression, other.expression);
    }
}
