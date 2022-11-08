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
package com.facebook.presto.functionNamespace.json;

import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * The function meta data provided by the Json file to the {@link JsonFileBasedFunctionNamespaceManager}.
 */
public class JsonBasedUdfFunctionMetadata
{
    /**
     * Description of the function.
     */
    private final String docString;
    /**
     * Output type of the function.
     */
    private final String outputType;
    /**
     * Input types of the function
     */
    private final List<String> paramTypes;
    /**
     * Schema the function belongs to. Catalog.schema.function uniquely identifies a function.
     */
    private final String schema;
    /**
     * Implement language of the function.
     */
    private final RoutineCharacteristics routineCharacteristics;

    @JsonCreator
    public JsonBasedUdfFunctionMetadata(
            @JsonProperty("docString") String docString,
            @JsonProperty("outputType") String outputType,
            @JsonProperty("paramTypes") List<String> paramTypes,
            @JsonProperty("schema") String schema,
            @JsonProperty("routineCharacteristics") RoutineCharacteristics routineCharacteristics)
    {
        this.docString = requireNonNull(docString, "docString is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.paramTypes = ImmutableList.copyOf(requireNonNull(paramTypes, "paramTypes is null"));
        this.schema = requireNonNull(schema, "schema is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
    }

    public String getDocString()
    {
        return docString;
    }

    public String getOutputType()
    {
        return outputType;
    }

    public List<String> getParamNames()
    {
        return IntStream.range(0, paramTypes.size()).boxed().map(idx -> "input" + idx).collect(toImmutableList());
    }

    public List<String> getParamTypes()
    {
        return paramTypes;
    }

    public String getSchema()
    {
        return schema;
    }

    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }
}
