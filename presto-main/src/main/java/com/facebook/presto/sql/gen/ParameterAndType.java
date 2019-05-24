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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.Parameter;

import static java.util.Objects.requireNonNull;

class ParameterAndType
{
    private final Parameter parameter;
    private final Class<?> type;

    public ParameterAndType(Parameter parameter, Class<?> type)
    {
        this.parameter = requireNonNull(parameter, "parameter is null");
        this.type = requireNonNull(type, "type is null");
    }

    public Parameter getParameter()
    {
        return parameter;
    }

    public Class<?> getType()
    {
        return type;
    }
}
