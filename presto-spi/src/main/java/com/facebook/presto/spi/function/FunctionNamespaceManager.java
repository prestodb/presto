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

import com.facebook.presto.spi.api.Experimental;
import com.facebook.presto.spi.relation.FullyQualifiedName;

import java.util.Collection;
import java.util.Optional;

@Experimental
public interface FunctionNamespaceManager<F extends SqlFunction>
{
    String getName();

    /**
     * Start a transaction.
     */
    FunctionNamespaceTransactionHandle beginTransaction();

    /**
     * Commit the transaction. Will be called at most once and will not be called if
     * {@link #rollback(FunctionNamespaceTransactionHandle)} is called.
     */
    void commit(FunctionNamespaceTransactionHandle transactionHandle);

    /**
     * Rollback the transaction. Will be called at most once and will not be called if
     * {@link #commit(FunctionNamespaceTransactionHandle)} is called.
     */
    void rollback(FunctionNamespaceTransactionHandle transactionHandle);

    /**
     * Create or replace the specified function.
     * TODO: Support transaction
     */
    void createFunction(F function, boolean replace);

    /**
     * List all functions managed by the {@link FunctionNamespaceManager}.
     * TODO: Support transaction
     */
    Collection<F> listFunctions();

    Collection<F> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, FullyQualifiedName functionName);

    FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature);

    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);

    ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle);
}
