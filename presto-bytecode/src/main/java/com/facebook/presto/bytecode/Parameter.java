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
package com.facebook.presto.bytecode;

import javax.annotation.concurrent.Immutable;

@Immutable
public class Parameter
        extends Variable
{
    public static Parameter arg(String name, Class<?> type)
    {
        return new Parameter(name, ParameterizedType.type(type));
    }

    public static Parameter arg(String name, ParameterizedType type)
    {
        return new Parameter(name, type);
    }

    Parameter(String name, ParameterizedType type)
    {
        super(name, type);
    }

    String getSourceString()
    {
        return getType().getJavaClassName() + " " + getName();
    }
}
