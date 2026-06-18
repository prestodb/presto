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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.SqlFunction;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.metadata.BuiltInFunctionKind.WORKER;
import static com.facebook.presto.spi.function.FunctionImplementationType.CPP;

public class BuiltInWorkerFunctionNamespaceManager
        extends BuiltInSpecialFunctionNamespaceManager
{
    public BuiltInWorkerFunctionNamespaceManager(FunctionAndTypeManager functionAndTypeManager)
    {
        super(functionAndTypeManager);
    }

    @Override
    public synchronized void registerBuiltInSpecialFunctions(List<? extends SqlFunction> functions)
    {
        // only register functions once
        if (!this.functions.list().isEmpty()) {
            return;
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    @Override
    protected synchronized void checkForNamingConflicts(Collection<? extends SqlFunction> functions)
    {
    }

    @Override
    protected BuiltInFunctionKind getBuiltInFunctionKind()
    {
        return WORKER;
    }

    @Override
    protected FunctionImplementationType getDefaultFunctionMetadataImplementationType()
    {
        return CPP;
    }
}
