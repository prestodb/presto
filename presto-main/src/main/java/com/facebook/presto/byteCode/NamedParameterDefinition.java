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
package com.facebook.presto.byteCode;

import com.google.common.base.Function;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class NamedParameterDefinition
{
    public static NamedParameterDefinition arg(Class<?> type)
    {
        return new NamedParameterDefinition(null, ParameterizedType.type(type));
    }

    public static NamedParameterDefinition arg(String name, Class<?> type)
    {
        return new NamedParameterDefinition(name, ParameterizedType.type(type));
    }

    public static NamedParameterDefinition arg(ParameterizedType type)
    {
        return new NamedParameterDefinition(null, type);
    }

    public static NamedParameterDefinition arg(String name, ParameterizedType type)
    {
        return new NamedParameterDefinition(name, type);
    }

    private final ParameterizedType type;
    private final String name;

    NamedParameterDefinition(String name, ParameterizedType type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public ParameterizedType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("NamedParameterDefinition");
        sb.append("{name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public static Function<NamedParameterDefinition, ParameterizedType> getNamedParameterType()
    {
        return new Function<NamedParameterDefinition, ParameterizedType>()
        {
            @Override
            public ParameterizedType apply(@Nullable NamedParameterDefinition input)
            {
                return input.getType();
            }
        };
    }
}
