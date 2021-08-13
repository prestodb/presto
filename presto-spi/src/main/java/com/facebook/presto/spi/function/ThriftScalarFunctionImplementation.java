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

import com.facebook.presto.spi.function.RoutineCharacteristics.Language;

import static java.util.Objects.requireNonNull;

public class ThriftScalarFunctionImplementation
        implements ScalarFunctionImplementation
{
    private final SqlFunctionHandle functionHandle;
    private final Language language;

    public ThriftScalarFunctionImplementation(SqlFunctionHandle functionHandle, Language language)
    {
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.language = requireNonNull(language, "language is null");
    }

    public SqlFunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    public Language getLanguage()
    {
        return language;
    }
}
