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

import java.util.Objects;
import java.util.function.Function;

public class AllExpression<C>
        extends TupleExpression<C>
{
    @JsonCreator
    public AllExpression()
    {}

    @Override
    public boolean isAll()
    {
        return true;
    }

    @Override
    public boolean isNone()
    {
        return false;
    }

    @Override
    public String getName()
    {
        return "all";
    }

    @Override
    public <U> TupleExpression<U> transform(Function<C, U> function)
    {
        return new AllExpression<U>();
    }

    @Override
    public <R, T> R accept(TupleExpressionVisitor<R, T> visitor, T context)
    {
        return visitor.visitAllExpression(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode("all");
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }
}
