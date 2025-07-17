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
package com.facebook.presto.functionNamespace;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JsonBasedUdfFunctionMetadata
{
    /**
     * Description of the function.
     */
    private final String docString;
    /**
     * FunctionKind of the function (e.g. SCALAR, AGGREGATE)
     */
    private final FunctionKind functionKind;
    /**
     * Output type of the function.
     */
    private final TypeSignature outputType;
    /**
     * Input types of the function
     */
    private final List<TypeSignature> paramTypes;
    /**
     * Schema the function belongs to. Catalog.schema.function uniquely identifies a function.
     */
    private final String schema;
    /**
     * Implement language of the function.
     */
    private final RoutineCharacteristics routineCharacteristics;

    /**
     * Optional Aggregate-specific metadata (required for aggregation functions)
     */
    private final Optional<AggregationFunctionMetadata> aggregateMetadata;
    /**
     * Marked to indicate whether it is a variable arity function.
     * A variable arity function can have a variable number of arguments of the specified type.
     */
    private final boolean variableArity;
    /**
     * Optional list of the typeVariableConstraints.
     */
    private final Optional<List<TypeVariableConstraint>> typeVariableConstraints;
    private final Optional<List<LongVariableConstraint>> longVariableConstraints;
    private final Optional<SqlFunctionId> functionId;
    private final Optional<String> version;
    /**
     * Optional execution endpoint for routing function execution to a different server
     */
    private final Optional<URI> executionEndpoint;

    @JsonCreator
    public JsonBasedUdfFunctionMetadata(
            @JsonProperty("docString") String docString,
            @JsonProperty("functionKind") FunctionKind functionKind,
            @JsonProperty("outputType") TypeSignature outputType,
            @JsonProperty("paramTypes") List<TypeSignature> paramTypes,
            @JsonProperty("schema") String schema,
            @JsonProperty("variableArity") boolean variableArity,
            @JsonProperty("routineCharacteristics") RoutineCharacteristics routineCharacteristics,
            @JsonProperty("aggregateMetadata") Optional<AggregationFunctionMetadata> aggregateMetadata,
            @JsonProperty("functionId") Optional<SqlFunctionId> functionId,
            @JsonProperty("version") Optional<String> version,
            @JsonProperty("typeVariableConstraints") Optional<List<TypeVariableConstraint>> typeVariableConstraints,
            @JsonProperty("longVariableConstraints") Optional<List<LongVariableConstraint>> longVariableConstraints,
            @JsonProperty("executionEndpoint") Optional<URI> executionEndpoint)
    {
        this.docString = requireNonNull(docString, "docString is null");
        this.functionKind = requireNonNull(functionKind, "functionKind is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.paramTypes = ImmutableList.copyOf(requireNonNull(paramTypes, "paramTypes is null"));
        this.schema = requireNonNull(schema, "schema is null");
        this.variableArity = variableArity;
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.aggregateMetadata = requireNonNull(aggregateMetadata, "aggregateMetadata is null");
        checkArgument(
                (functionKind == AGGREGATE && aggregateMetadata.isPresent()) || (functionKind != AGGREGATE && !aggregateMetadata.isPresent()),
                "aggregateMetadata must be present for aggregation functions and absent otherwise");
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.version = requireNonNull(version, "version is null");
        this.typeVariableConstraints = requireNonNull(typeVariableConstraints, "typeVariableConstraints is null");
        this.longVariableConstraints = requireNonNull(longVariableConstraints, "longVariableConstraints is null");
        this.executionEndpoint = requireNonNull(executionEndpoint, "executionEndpoint is null");
        executionEndpoint.ifPresent(uri -> {
            String scheme = uri.getScheme();
            if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
                throw new IllegalArgumentException("Execution endpoint must use HTTP or HTTPS protocol: " + uri);
            }
        });
    }

    @JsonProperty
    public String getDocString()
    {
        return docString;
    }

    @JsonProperty
    public FunctionKind getFunctionKind()
    {
        return functionKind;
    }

    @JsonProperty
    public TypeSignature getOutputType()
    {
        return outputType;
    }

    @JsonIgnore
    public List<String> getParamNames()
    {
        return IntStream.range(0, paramTypes.size()).boxed().map(idx -> "input" + idx).collect(toImmutableList());
    }

    @JsonProperty
    public List<TypeSignature> getParamTypes()
    {
        return paramTypes;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public boolean getVariableArity()
    {
        return variableArity;
    }

    @JsonProperty
    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    @JsonProperty
    public Optional<AggregationFunctionMetadata> getAggregateMetadata()
    {
        return aggregateMetadata;
    }

    @JsonProperty
    public Optional<SqlFunctionId> getFunctionId()
    {
        return functionId;
    }

    @JsonProperty
    public Optional<String> getVersion()
    {
        return version;
    }

    @JsonProperty
    public Optional<List<TypeVariableConstraint>> getTypeVariableConstraints()
    {
        return typeVariableConstraints;
    }

    @JsonProperty
    public Optional<List<LongVariableConstraint>> getLongVariableConstraints()
    {
        return longVariableConstraints;
    }

    @JsonProperty
    public Optional<URI> getExecutionEndpoint()
    {
        return executionEndpoint;
    }
}
