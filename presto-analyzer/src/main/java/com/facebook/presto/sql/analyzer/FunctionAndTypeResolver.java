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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface FunctionAndTypeResolver
        extends TypeManager, FunctionMetadataManager
{
    FunctionHandle resolveOperator(OperatorType operatorType, List<TypeSignatureProvider> argumentTypes);

    FunctionHandle lookupFunction(String functionName, List<TypeSignatureProvider> fromTypes);

    FunctionHandle resolveFunction(
            Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions,
            Optional<TransactionId> transactionId,
            QualifiedObjectName functionName,
            List<TypeSignatureProvider> parameterTypes);

    Collection<SqlFunction> listBuiltInFunctions();

    Optional<Type> getCommonSuperType(Type firstType, Type secondType);

    boolean isTypeOnlyCoercion(Type actualType, Type expectedType);

    FunctionHandle lookupCast(String castType, Type fromType, Type toType);
}
