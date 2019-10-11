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
package com.facebook.presto.testing;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.QualifiedFunctionName;
import com.facebook.presto.sqlfunction.AbstractSqlInvokedFunctionNamespaceManager;
import com.facebook.presto.sqlfunction.SqlFunctionId;
import com.facebook.presto.sqlfunction.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.sqlfunction.SqlInvokedRegularFunction;
import com.facebook.presto.sqlfunction.SqlInvokedRegularFunctionHandle;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.lang.String.format;

@ThreadSafe
public class InMemoryFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private static final String NAME = "_in_memory";
    private final Map<SqlFunctionId, SqlInvokedRegularFunction> latestFunctions = new ConcurrentHashMap<>();

    public InMemoryFunctionNamespaceManager(SqlInvokedFunctionNamespaceManagerConfig config)
    {
        super(config);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public synchronized void createFunction(SqlInvokedRegularFunction function, boolean replace)
    {
        SqlFunctionId functionId = new SqlFunctionId(function.getSignature().getName(), function.getSignature().getArgumentTypes());
        if (!replace && latestFunctions.containsKey(functionId)) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Function '%s' already exists", functionId.getName()));
        }

        SqlInvokedRegularFunction replacedFunction = latestFunctions.get(functionId);
        long version = 1;
        if (replacedFunction != null) {
            checkArgument(replacedFunction.getVersion().isPresent(), "missing version in replaced function");
            version = replacedFunction.getVersion().get() + 1;
        }
        function = SqlInvokedRegularFunction.versioned(function, version);
        latestFunctions.put(functionId, function);
    }

    @Override
    public Collection<SqlInvokedRegularFunction> listFunctions()
    {
        return latestFunctions.values();
    }

    @Override
    public Collection<SqlInvokedRegularFunction> fetchFunctionsDirect(QualifiedFunctionName name)
    {
        return latestFunctions.values().stream()
                .filter(function -> function.getSignature().getName().equals(name))
                .map(InMemoryFunctionNamespaceManager::copyFunction)
                .collect(toImmutableList());
    }

    @Override
    public FunctionMetadata fetchFunctionMetadataDirect(SqlInvokedRegularFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(AbstractSqlInvokedFunctionNamespaceManager::sqlInvokedFunctionToMetadata)
                .collect(onlyElement());
    }

    private static SqlInvokedRegularFunction copyFunction(SqlInvokedRegularFunction function)
    {
        return new SqlInvokedRegularFunction(
                function.getSignature().getName(),
                function.getParameters(),
                function.getSignature().getReturnType(),
                function.getComment(),
                function.getRoutineCharacteristics(),
                function.getBody(),
                function.getVersion());
    }
}
