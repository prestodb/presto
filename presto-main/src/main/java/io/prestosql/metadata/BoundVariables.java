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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class BoundVariables
{
    private final Map<String, Type> typeVariables;
    private final Map<String, Long> longVariables;

    public BoundVariables(Map<String, Type> typeVariables,
            Map<String, Long> longVariables)
    {
        requireNonNull(typeVariables, "typeVariableBindings is null");
        requireNonNull(longVariables, "longVariableBindings is null");
        this.typeVariables = ImmutableMap.copyOf(typeVariables);
        this.longVariables = ImmutableMap.copyOf(longVariables);
    }

    public Type getTypeVariable(String variableName)
    {
        return getValue(typeVariables, variableName);
    }

    public boolean containsTypeVariable(String variableName)
    {
        return containsValue(typeVariables, variableName);
    }

    public Map<String, Type> getTypeVariables()
    {
        return typeVariables;
    }

    public Long getLongVariable(String variableName)
    {
        return getValue(longVariables, variableName);
    }

    public boolean containsLongVariable(String variableName)
    {
        return containsValue(longVariables, variableName);
    }

    public Map<String, Long> getLongVariables()
    {
        return longVariables;
    }

    private static <T> T getValue(Map<String, T> map, String variableName)
    {
        checkState(variableName != null, "variableName is null");
        T value = map.get(variableName);
        checkState(value != null, "value for variable '%s' is null", variableName);
        return value;
    }

    private static boolean containsValue(Map<String, ?> map, String variableName)
    {
        checkState(variableName != null, "variableName is null");
        return map.containsKey(variableName);
    }

    private static <T> void setValue(Map<String, T> map, String variableName, T value)
    {
        checkState(variableName != null, "variableName is null");
        checkState(value != null, "value for variable '%s' is null", variableName);
        map.put(variableName, value);
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
        BoundVariables that = (BoundVariables) o;
        return Objects.equals(typeVariables, that.typeVariables) &&
                Objects.equals(longVariables, that.longVariables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeVariables, longVariables);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("typeVariables", typeVariables)
                .add("longVariables", longVariables)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final Map<String, Type> typeVariables = new HashMap<>();
        private final Map<String, Long> longVariables = new HashMap<>();

        public Type getTypeVariable(String variableName)
        {
            return getValue(typeVariables, variableName);
        }

        public Builder setTypeVariable(String variableName, Type variableValue)
        {
            setValue(typeVariables, variableName, variableValue);
            return this;
        }

        public boolean containsTypeVariable(String variableName)
        {
            return containsValue(typeVariables, variableName);
        }

        public Map<String, Type> getTypeVariables()
        {
            return typeVariables;
        }

        public Long getLongVariable(String variableName)
        {
            return getValue(longVariables, variableName);
        }

        public Builder setLongVariable(String variableName, Long variableValue)
        {
            setValue(longVariables, variableName, variableValue);
            return this;
        }

        public boolean containsLongVariable(String variableName)
        {
            return containsValue(longVariables, variableName);
        }

        public Map<String, Long> getLongVariables()
        {
            return longVariables;
        }

        public BoundVariables build()
        {
            return new BoundVariables(typeVariables, longVariables);
        }
    }
}
