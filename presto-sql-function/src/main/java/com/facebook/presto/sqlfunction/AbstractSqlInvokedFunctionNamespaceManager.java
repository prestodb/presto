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
package com.facebook.presto.sqlfunction;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.FullyQualifiedName;

import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public abstract class AbstractSqlInvokedFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlInvokedRegularFunction>
{
    private final ConcurrentMap<FunctionNamespaceTransactionHandle, FunctionCollection> transactions = new ConcurrentHashMap<>();

    protected abstract Collection<SqlInvokedRegularFunction> fetchFunctions(FullyQualifiedName functionName);

    @Override
    public final FunctionNamespaceTransactionHandle beginTransaction()
    {
        UuidFunctionNamespaceTransactionHandle transactionHandle = UuidFunctionNamespaceTransactionHandle.create();
        transactions.put(transactionHandle, new FunctionCollection());
        return transactionHandle;
    }

    @Override
    public final void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional commit is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final void rollback(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional rollback is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final Collection<SqlInvokedRegularFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, FullyQualifiedName functionName)
    {
        checkArgument(transactionHandle.isPresent(), "missing transactionHandle");
        return transactions.get(transactionHandle.get()).loadAndGetFunctionsTransactional(functionName);
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        checkArgument(transactionHandle.isPresent(), "missing transactionHandle");
        SqlFunctionId functionId = new SqlFunctionId(signature.getName(), signature.getArgumentTypes());
        return transactions.get(transactionHandle.get()).getFunctionHandle(functionId);
    }

    private class FunctionCollection
    {
        @GuardedBy("this")
        private final Map<FullyQualifiedName, Collection<SqlInvokedRegularFunction>> functions = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<SqlFunctionId, SqlInvokedRegularFunctionHandle> functionHandles = new ConcurrentHashMap<>();

        public synchronized Collection<SqlInvokedRegularFunction> loadAndGetFunctionsTransactional(FullyQualifiedName functionName)
        {
            Collection<SqlInvokedRegularFunction> functions = this.functions.computeIfAbsent(functionName, AbstractSqlInvokedFunctionNamespaceManager.this::fetchFunctions);
            functionHandles.putAll(functions.stream().collect(toImmutableMap(SqlInvokedRegularFunction::getFunctionId, SqlInvokedRegularFunction::getRequiredFunctionHandle)));
            return functions;
        }

        public synchronized FunctionHandle getFunctionHandle(SqlFunctionId functionId)
        {
            return functionHandles.get(functionId);
        }
    }
}
