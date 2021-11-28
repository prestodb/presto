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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.spi.api.Experimental;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Experimental
public interface FunctionNamespaceManager<F extends SqlFunction>
{
    /**
     * BlockEncodingSerde might be needed to serialize/deserialize Presto pages when running external functions.
     */
    void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde);

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
    void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics);

    /**
     * Drop the specified function.
     * TODO: Support transaction
     */
    void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists);

    /**
     * List all functions managed by the {@link FunctionNamespaceManager}.
     * likePattern and escape are from `SHOW FUNCTIONS LIKE [likePattern] escape [escape]`.
     * Backends supporting like pattern / escape matching can use this to prefilter functions, but Presto will filter again, so it is fine if the backend doesn't
     * use these parameters.
     * TODO: Support transaction
     */
    Collection<F> listFunctions(Optional<String> likePattern, Optional<String> escape);

    /**
     * Whether this function namespace manager would do function resolution or not.
     *
     * If returns false, engine function resolution logic will call {@link #getFunctions(Optional, QualifiedObjectName)}
     * to get all candidates, and use engine logic to resolve to a specific function signature, then call {@link #getFunctionHandle(Optional, Signature)}
     * to get the function handle.
     *
     * If returns true, the function resolution logic will be delegated to the function namespace manager, and the engine
     * will call {@link #resolveFunction(Optional, QualifiedObjectName, List)} directly to get the function handle.
     *
     */
    default boolean canResolveFunction()
    {
        return false;
    }

    /**
     * When {@link #canResolveFunction()} returns true, this function is called to resolve the function to a function handle.
     * This is useful for plugins like Hive functions because the function resolution logic is defined with the function itself.
     */
    default FunctionHandle resolveFunction(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName, List<TypeSignature> parameterTypes)
    {
        throw new UnsupportedOperationException("Does not support resolving function");
    }

    Collection<F> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName);

    FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature);

    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);

    ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle);

    CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager);

    void addUserDefinedType(UserDefinedType userDefinedType);

    Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName);
}
