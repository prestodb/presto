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

import com.facebook.presto.common.predicate.Primitives;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ScalarArgumentSpecification
        extends ArgumentSpecification
{
    public static final String argumentType = "ScalarArgumentSpecification";
    private final Type type;

    @JsonCreator
    public ScalarArgumentSpecification(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("required") boolean required,
            @Nullable Object defaultValue)
    {
        super(name, required, defaultValue);
        this.type = requireNonNull(type, "type is null");
        if (defaultValue != null) {
            checkArgument(Primitives.wrap(type.getJavaType()).isInstance(defaultValue), format("default value %s does not match the declared type: %s", defaultValue, type));
        }
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;
        private Type type;
        private boolean required = true;
        private Object defaultValue;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder type(Type type)
        {
            this.type = type;
            return this;
        }

        public Builder defaultValue(Object defaultValue)
        {
            this.required = false;
            this.defaultValue = defaultValue;
            return this;
        }

        public ScalarArgumentSpecification build()
        {
            return new ScalarArgumentSpecification(name, type, required, defaultValue);
        }
    }

    @Override
    public String getArgumentType()
    {
        return argumentType;
    }
}
