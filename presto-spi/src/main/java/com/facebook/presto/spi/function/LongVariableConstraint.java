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
package com.facebook.presto.spi.function;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.experimental.auto_gen.ThriftLongVariableConstraint;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@ThriftStruct
public class LongVariableConstraint
{
    private final String name;
    private final String expression;

    public LongVariableConstraint(ThriftLongVariableConstraint thriftConstraint)
    {
        this(thriftConstraint.getName(), thriftConstraint.getExpression());
    }

    public ThriftLongVariableConstraint toThrift()
    {
        return new ThriftLongVariableConstraint(name, expression);
    }

    @ThriftConstructor
    @JsonCreator
    public LongVariableConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("expression") String expression)
    {
        this.name = name;
        this.expression = expression;
    }

    @ThriftField(1)
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @ThriftField(2)
    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    @Override
    public String toString()
    {
        return name + ":" + expression;
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
        LongVariableConstraint that = (LongVariableConstraint) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expression);
    }
}
