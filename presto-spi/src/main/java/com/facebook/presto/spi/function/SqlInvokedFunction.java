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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Experimental
public class SqlInvokedFunction
        implements SqlFunction
{
    private final List<Parameter> parameters;
    private final String description;
    private final RoutineCharacteristics routineCharacteristics;
    private final String body;

    private final Signature signature;
    private final SqlFunctionId functionId;
    private final Optional<SqlFunctionHandle> functionHandle;

    public SqlInvokedFunction(
            QualifiedFunctionName functionName,
            List<Parameter> parameters,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            Optional<Long> version)
    {
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.description = requireNonNull(description, "description is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.body = requireNonNull(body, "body is null");

        List<TypeSignature> argumentTypes = parameters.stream()
                .map(Parameter::getType)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
        this.signature = new Signature(functionName, SCALAR, returnType, argumentTypes);
        this.functionId = new SqlFunctionId(functionName, argumentTypes);
        this.functionHandle = version.map(v -> new SqlFunctionHandle(this.functionId, v));
    }

    public SqlInvokedFunction withVersion(long version)
    {
        if (getVersion().isPresent()) {
            throw new IllegalArgumentException(format("function %s is already with version %s", signature.getName(), getVersion().get()));
        }
        return new SqlInvokedFunction(
                signature.getName(),
                parameters,
                signature.getReturnType(),
                description,
                routineCharacteristics,
                body,
                Optional.of(version));
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return routineCharacteristics.isDeterministic();
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return routineCharacteristics.isCalledOnNullInput();
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    public List<Parameter> getParameters()
    {
        return parameters;
    }

    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public String getBody()
    {
        return body;
    }

    public SqlFunctionId getFunctionId()
    {
        return functionId;
    }

    public Optional<SqlFunctionHandle> getFunctionHandle()
    {
        return functionHandle;
    }

    public Optional<Long> getVersion()
    {
        return functionHandle.map(SqlFunctionHandle::getVersion);
    }

    public FunctionImplementationType getFunctionImplementationType()
    {
        if (routineCharacteristics.getLanguage().equals(SQL)) {
            return FunctionImplementationType.SQL;
        }
        return FunctionImplementationType.THRIFT;
    }

    public SqlFunctionHandle getRequiredFunctionHandle()
    {
        Optional<? extends SqlFunctionHandle> functionHandle = getFunctionHandle();
        if (!functionHandle.isPresent()) {
            throw new IllegalStateException("missing functionHandle");
        }
        return functionHandle.get();
    }

    public long getRequiredVersion()
    {
        Optional<Long> version = getVersion();
        if (!version.isPresent()) {
            throw new IllegalStateException("missing version");
        }
        return version.get();
    }

    public boolean hasSameDefinitionAs(SqlInvokedFunction function)
    {
        if (function == null) {
            throw new IllegalArgumentException("function is null");
        }
        return Objects.equals(parameters, function.parameters)
                && Objects.equals(description, function.description)
                && Objects.equals(routineCharacteristics, function.routineCharacteristics)
                && Objects.equals(body, function.body)
                && Objects.equals(signature, function.signature);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlInvokedFunction o = (SqlInvokedFunction) obj;
        return Objects.equals(parameters, o.parameters)
                && Objects.equals(description, o.description)
                && Objects.equals(routineCharacteristics, o.routineCharacteristics)
                && Objects.equals(body, o.body)
                && Objects.equals(signature, o.signature)
                && Objects.equals(functionId, o.functionId)
                && Objects.equals(functionHandle, o.functionHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parameters, description, routineCharacteristics, body, signature, functionId, functionHandle);
    }

    @Override
    public String toString()
    {
        return format(
                "%s(%s):%s%s {%s} %s",
                signature.getName(),
                parameters.stream()
                        .map(Object::toString)
                        .collect(joining(",")),
                signature.getReturnType(),
                getVersion().map(version -> ":" + version).orElse(""),
                body,
                routineCharacteristics);
    }
}
