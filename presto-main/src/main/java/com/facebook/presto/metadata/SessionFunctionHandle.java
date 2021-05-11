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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SessionFunctionHandle
        implements FunctionHandle
{
    public static final CatalogSchemaName SESSION_NAMESPACE = new CatalogSchemaName("presto", "session");

    private final SqlInvokedFunction sqlFunction;

    @JsonCreator
    public SessionFunctionHandle(@JsonProperty("sqlFunction") SqlInvokedFunction sqlFunction)
    {
        this.sqlFunction = requireNonNull(sqlFunction, "sqlFunction is null");
    }

    @Override
    public CatalogSchemaName getCatalogSchemaName()
    {
        return SESSION_NAMESPACE;
    }

    public FunctionMetadata getFunctionMetadata()
    {
        Signature signature = sqlFunction.getSignature();
        return new FunctionMetadata(
                signature.getName(),
                signature.getArgumentTypes(),
                sqlFunction.getParameters().stream().map(Parameter::getName).collect(toImmutableList()),
                signature.getReturnType(),
                signature.getKind(),
                sqlFunction.getRoutineCharacteristics().getLanguage(),
                FunctionImplementationType.SQL,
                sqlFunction.isDeterministic(),
                sqlFunction.isCalledOnNullInput(),
                sqlFunction.getVersion());
    }

    @JsonProperty
    public SqlInvokedFunction getSqlFunction()
    {
        return sqlFunction;
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation()
    {
        return new SqlInvokedScalarFunctionImplementation(sqlFunction.getBody());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionFunctionHandle that = (SessionFunctionHandle) o;
        return Objects.equals(sqlFunction, that.sqlFunction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sqlFunction);
    }
}
