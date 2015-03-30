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

import javax.annotation.concurrent.Immutable;

@Immutable
public class Parameter
{
    public static Parameter arg(Class<?> type)
    {
        return new Parameter(null, ParameterizedType.type(type));
    }

    public static Parameter arg(String name, Class<?> type)
    {
        return new Parameter(name, ParameterizedType.type(type));
    }

    public static Parameter arg(ParameterizedType type)
    {
        return new Parameter(null, type);
    }

    public static Parameter arg(String name, ParameterizedType type)
    {
        return new Parameter(name, type);
    }

    private final ParameterizedType type;
    private final String name;

    Parameter(String name, ParameterizedType type)
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
        StringBuilder sb = new StringBuilder();
        sb.append("NamedParameterDefinition");
        sb.append("{name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public String getSourceString()
    {
        return getType().getJavaClassName() + " " + getName();
    }
}
