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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * This class represents the scalar argument passed to a Table Function.
 * <p>
 * This representation should be considered experimental. Eventually, {@link ConnectorExpression}
 * should be extended to include this kind of argument.
 * <p>
 * Additionally, only constant values are currently supported. In the future,
 * we will add support for different kinds of expressions.
 */
public class ScalarArgument
        extends Argument
{
    private final Type type;

    // native representation
    private final Object value;

    @JsonCreator
    public ScalarArgument(@JsonProperty("type") Type type, @JsonProperty("value") Object value)
    {
        this.type = requireNonNull(type, "type is null");
        this.value = value;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Object getValue()
    {
        return value;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Type type;
        private Object value;

        private Builder() {}

        public Builder type(Type type)
        {
            this.type = type;
            return this;
        }

        public Builder value(Object value)
        {
            this.value = value;
            return this;
        }

        public ScalarArgument build()
        {
            return new ScalarArgument(type, value);
        }
    }
}
