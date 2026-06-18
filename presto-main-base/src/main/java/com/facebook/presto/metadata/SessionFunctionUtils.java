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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class SessionFunctionUtils
{
    private SessionFunctionUtils() {}

    public static Collection<? extends SqlFunction> listFunctions(Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        return sessionFunctions.values();
    }

    public static Collection<String> listFunctionNames(Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        return sessionFunctions.keySet().stream()
                .map(SqlFunctionId::getFunctionName)
                .map(QualifiedObjectName::getObjectName)
                .collect(toImmutableList());
    }

    public static Collection<SqlFunction> getFunctions(Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, QualifiedObjectName functionName)
    {
        return sessionFunctions.entrySet().stream()
                .filter(e -> e.getKey().getFunctionName().getObjectName().equals(functionName.getObjectName()))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
    }

    public static FunctionHandle getFunctionHandle(Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions, Signature signature)
    {
        return new SessionFunctionHandle(sessionFunctions.get(new SqlFunctionId(signature.getName(), signature.getArgumentTypes())));
    }
}
