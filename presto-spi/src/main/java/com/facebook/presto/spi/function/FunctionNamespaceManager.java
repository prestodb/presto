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

import com.facebook.presto.common.function.QualifiedFunctionName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.api.Experimental;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Experimental
public interface FunctionNamespaceManager<F extends SqlFunction>
{
    /**
     * Start a transaction.
     */
    FunctionNamespaceTransactionHandle beginTransaction();

    /**
     * Commit the transaction. Will be called at most once and will not be called if
     * {@link #abort(FunctionNamespaceTransactionHandle)} is called.
     */
    void commit(FunctionNamespaceTransactionHandle transactionHandle);

    /**
     * Rollback the transaction. Will be called at most once and will not be called if
     * {@link #commit(FunctionNamespaceTransactionHandle)} is called.
     */
    void abort(FunctionNamespaceTransactionHandle transactionHandle);

    /**
     * Create or replace the specified function.
     * TODO: Support transaction
     */
    void createFunction(SqlInvokedFunction function, boolean replace);

    /**
     * Alter the specified function.
     * TODO: Support transaction
     */
    void alterFunction(QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics);

    /**
     * Drop the specified function.
     * TODO: Support transaction
     */
    void dropFunction(QualifiedFunctionName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists);

    /**
     * List all functions managed by the {@link FunctionNamespaceManager}.
     * TODO: Support transaction
     */
    Collection<F> listFunctions();

    Collection<F> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedFunctionName functionName);

    FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature);

    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);

    ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle);
}
