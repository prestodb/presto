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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.api.Experimental;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
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
    private final FunctionVersion functionVersion;
    private final Optional<SqlFunctionHandle> functionHandle;

    @JsonCreator
    public SqlInvokedFunction(
            @JsonProperty("parameters") List<Parameter> parameters,
            @JsonProperty("description") String description,
            @JsonProperty("routineCharacteristics") RoutineCharacteristics routineCharacteristics,
            @JsonProperty("body") String body,
            @JsonProperty("signature") Signature signature,
            @JsonProperty("functionId") SqlFunctionId functionId)
    {
        this.parameters = parameters;
        this.description = description;
        this.routineCharacteristics = routineCharacteristics;
        this.body = body;
        this.signature = signature;
        this.functionId = functionId;
        this.functionVersion = notVersioned();
        this.functionHandle = Optional.empty();
    }

    public SqlInvokedFunction(
            QualifiedObjectName functionName,
            List<Parameter> parameters,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            FunctionVersion version)
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
        this.functionVersion = requireNonNull(version, "version is null");
        this.functionHandle = version.hasVersion() ? Optional.of(new SqlFunctionHandle(this.functionId, version.toString())) : Optional.empty();
    }

    public SqlInvokedFunction withVersion(String version)
    {
        if (hasVersion()) {
            throw new IllegalArgumentException(format("function %s is already with version %s", signature.getName(), getVersion()));
        }
        return new SqlInvokedFunction(
                signature.getName(),
                parameters,
                signature.getReturnType(),
                description,
                routineCharacteristics,
                body,
                FunctionVersion.withVersion(version));
    }

    @Override
    @JsonProperty
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
    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    public List<Parameter> getParameters()
    {
        return parameters;
    }

    @JsonProperty
    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    @JsonProperty
    public String getBody()
    {
        return body;
    }

    @JsonProperty
    public SqlFunctionId getFunctionId()
    {
        return functionId;
    }

    public Optional<SqlFunctionHandle> getFunctionHandle()
    {
        return functionHandle;
    }

    public boolean hasVersion()
    {
        return functionVersion.hasVersion();
    }

    public FunctionVersion getVersion()
    {
        return functionVersion;
    }

    public SqlFunctionHandle getRequiredFunctionHandle()
    {
        Optional<? extends SqlFunctionHandle> functionHandle = getFunctionHandle();
        if (!functionHandle.isPresent()) {
            throw new IllegalStateException("missing functionHandle");
        }
        return functionHandle.get();
    }

    public String getRequiredVersion()
    {
        if (!hasVersion()) {
            throw new IllegalStateException("missing version");
        }
        return getVersion().toString();
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
                hasVersion() ? ":" + getVersion() : "",
                body,
                routineCharacteristics);
    }
}
