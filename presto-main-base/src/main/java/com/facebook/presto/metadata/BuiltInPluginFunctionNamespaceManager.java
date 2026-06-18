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

import static com.facebook.presto.metadata.BuiltInFunctionKind.PLUGIN;
import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.google.common.base.Preconditions.checkArgument;

public class BuiltInPluginFunctionNamespaceManager
        extends BuiltInSpecialFunctionNamespaceManager
{
    public BuiltInPluginFunctionNamespaceManager(FunctionAndTypeManager functionAndTypeManager)
    {
        super(functionAndTypeManager);
    }

    public void triggerConflictCheckWithBuiltInFunctions()
    {
        checkForNamingConflicts(this.getFunctionsFromDefaultNamespace());
    }

    @Override
    public synchronized void registerBuiltInSpecialFunctions(List<? extends SqlFunction> functions)
    {
        checkForNamingConflicts(functions);
        this.functions = new FunctionMap(this.functions, functions);
    }

    @Override
    protected synchronized void checkForNamingConflicts(Collection<? extends SqlFunction> functions)
    {
        for (SqlFunction function : functions) {
            for (SqlFunction existingFunction : this.functions.list()) {
                checkArgument(!function.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", function.getSignature());
            }
        }
    }

    @Override
    protected BuiltInFunctionKind getBuiltInFunctionKind()
    {
        return PLUGIN;
    }

    @Override
    protected FunctionImplementationType getDefaultFunctionMetadataImplementationType()
    {
        return SQL;
    }
}
