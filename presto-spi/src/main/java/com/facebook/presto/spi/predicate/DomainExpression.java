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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class DomainExpression<C>
        extends TupleExpression<C>
{
    private final C column;
    private final Domain domain;

    @JsonCreator
    public DomainExpression(@JsonProperty("column") C column, @JsonProperty("domain") Domain domain)
    {
        this.column = column;
        this.domain = domain;
    }

    @JsonProperty
    public C getColumn()
    {
        return column;
    }

    @JsonProperty
    public Domain getDomain()
    {
        return domain;
    }

    @Override
    public Map<C, NullableValue> extractFixedValues()
    {
        Map<C, NullableValue> map = new HashMap();
        if (domain.isNullableSingleValue()) {
            map.put(column, new NullableValue(domain.getType(), domain.getNullableSingleValue()));
        }
        return map;
    }

    @Override
    public List<C> getColumnDomains()
    {
        return Arrays.asList(column);
    }

    @Override
    public <U> TupleExpression transform(Function<C, U> function)

    {
        U key = function.apply(column);
        //need to handle if key is null
        if (key != null) {
            return new DomainExpression<U>(key, domain);
        }
        else {
            return null;
        }
    }

    @Override
    public <R, T> R accept(TupleExpressionVisitor<R, T> visitor, T context)
    {
        return visitor.visitDomainExpression(this, context);
    }

    @Override
    public String getName()
    {
        return "domain";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, domain);
    }

    @Override
    public boolean isNone()
    {
        return domain.isNone();
    }

    @Override
    public boolean isAll()
    {
        return domain.isAll();
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
        DomainExpression other = (DomainExpression) obj;
        return Objects.equals(this.column, other.column) &&
                Objects.equals(this.domain, other.domain);
    }
}
