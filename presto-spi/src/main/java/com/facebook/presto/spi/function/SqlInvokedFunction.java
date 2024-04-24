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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.api.Experimental;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Experimental
@ThriftStruct
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

    public Optional<ScalarStatsHeader> getScalarStatsHeader()
    {
        return scalarStatsHeader;
    }

    private final Optional<ScalarStatsHeader> scalarStatsHeader;
    /**
     * Metadata required for Aggregation Functions
     */
    private final Optional<AggregationFunctionMetadata> aggregationMetadata;

    @ThriftConstructor
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
        this.aggregationMetadata = Optional.empty();
        this.scalarStatsHeader = Optional.empty();
    }

    // This constructor creates a SCALAR SqlInvokedFunction
    public SqlInvokedFunction(
            QualifiedObjectName functionName,
            List<Parameter> parameters,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            FunctionVersion version)
    {
        this(functionName, parameters, emptyList(), returnType, description, routineCharacteristics, body, version, SCALAR, Optional.empty());
    }

    public SqlInvokedFunction(
            QualifiedObjectName functionName,
            List<Parameter> parameters,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            FunctionVersion version,
            FunctionKind kind,
            Optional<AggregationFunctionMetadata> aggregationMetadata)
    {
        this(functionName, parameters, emptyList(), returnType, description, routineCharacteristics, body, version, kind, aggregationMetadata);
    }

    public SqlInvokedFunction(
            QualifiedObjectName functionName,
            List<Parameter> parameters,
            List<TypeVariableConstraint> typeVariableConstraints,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            FunctionVersion version,
            FunctionKind kind,
            Optional<AggregationFunctionMetadata> aggregationMetadata)
    {
        this(functionName, parameters, typeVariableConstraints, returnType, description, routineCharacteristics, body, version, kind, aggregationMetadata, Optional.empty());
    }

    public SqlInvokedFunction(
            QualifiedObjectName functionName,
            List<Parameter> parameters,
            List<TypeVariableConstraint> typeVariableConstraints,
            TypeSignature returnType,
            String description,
            RoutineCharacteristics routineCharacteristics,
            String body,
            FunctionVersion version,
            FunctionKind kind,
            Optional<AggregationFunctionMetadata> aggregationMetadata,
            Optional<ScalarStatsHeader> scalarStatsHeader)
    {
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.description = requireNonNull(description, "description is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.body = requireNonNull(body, "body is null");

        List<TypeSignature> argumentTypes = parameters.stream()
                .map(Parameter::getType)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));

        this.signature = new Signature(functionName, kind, typeVariableConstraints, emptyList(), returnType, argumentTypes, false);
        this.functionId = new SqlFunctionId(functionName, argumentTypes);
        this.functionVersion = requireNonNull(version, "version is null");
        this.functionHandle = version.hasVersion() ? Optional.of(new SqlFunctionHandle(this.functionId, version.toString())) : Optional.empty();
        this.aggregationMetadata = requireNonNull(aggregationMetadata, "aggregationMetadata is null");

        if ((kind == AGGREGATE && !aggregationMetadata.isPresent()) || (kind != AGGREGATE && aggregationMetadata.isPresent())) {
            throw new IllegalArgumentException("aggregationMetadata must be present for aggregation functions and absent otherwise");
        }
        this.scalarStatsHeader = scalarStatsHeader;
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
                FunctionVersion.withVersion(version),
                signature.getKind(),
                aggregationMetadata);
    }

    @Override
    @ThriftField(1)
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
    @ThriftField(2)
    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @ThriftField(3)
    @JsonProperty
    public List<Parameter> getParameters()
    {
        return parameters;
    }

    @ThriftField(4)
    @JsonProperty
    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    @ThriftField(5)
    @JsonProperty
    public String getBody()
    {
        return body;
    }

    @ThriftField(6)
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

    public Optional<AggregationFunctionMetadata> getAggregationMetadata()
    {
        return aggregationMetadata;
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
        requireNonNull(function, "function is null");

        return Objects.equals(parameters, function.parameters)
                && Objects.equals(description, function.description)
                && Objects.equals(routineCharacteristics, function.routineCharacteristics)
                && Objects.equals(body, function.body)
                && Objects.equals(signature, function.signature)
                && Objects.equals(aggregationMetadata, function.aggregationMetadata);
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
                && Objects.equals(functionHandle, o.functionHandle)
                && Objects.equals(aggregationMetadata, o.aggregationMetadata);
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
                "%s(%s):%s%s [%s%s] {%s} %s",
                signature.getName(),
                parameters.stream()
                        .map(Object::toString)
                        .collect(joining(",")),
                signature.getReturnType(),
                hasVersion() ? ":" + getVersion() : "",
                signature.getKind(),
                signature.getKind() == AGGREGATE ? ", " + getAggregationMetadata().get() : "",
                body,
                routineCharacteristics);
    }
}
