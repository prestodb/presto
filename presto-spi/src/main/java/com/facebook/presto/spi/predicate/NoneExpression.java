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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;
import java.util.function.Function;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NoneExpression<C>
        extends TupleExpression<C>
{
    @Override
    public <U> TupleExpression transform(Function<C, U> function)
    {
        return new NoneExpression<C>();
    }

    @Override
    public boolean isAll()
    {
        return false;
    }

    @Override
    public boolean isNone()
    {
        return true;
    }

    @Override
    public <R, T> R accept(TupleExpressionVisitor<R, T, C> visitor, T context)
    {
        return visitor.visitNoneExpression(this, context);
    }

    @Override
    public String getName()
    {
        return "none";
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode("none");
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public String toString()
    {
        return "NONE";
    }
}
