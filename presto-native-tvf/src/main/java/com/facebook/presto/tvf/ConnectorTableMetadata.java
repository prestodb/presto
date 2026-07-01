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
package com.facebook.presto.tvf;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.table.Argument;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Metadata for connector table function.
 */
public final class ConnectorTableMetadata
{
    private final QualifiedObjectName functionName;
    private final Map<String, Argument> arguments;

    /**
     * Constructs connector table metadata.
     *
     * @param functionName the function name
     * @param arguments the arguments
     */
    @JsonCreator
    public ConnectorTableMetadata(
            @JsonProperty("functionName")
            final QualifiedObjectName functionName,
            @JsonProperty("arguments")
            final Map<String, Argument> arguments)
    {
        this.functionName = requireNonNull(functionName,
                "functionName is null");
        this.arguments = ImmutableMap.copyOf(
                requireNonNull(arguments, "arguments is null"));
    }

    /**
     * Gets the function name.
     *
     * @return the function name
     */
    @JsonProperty("functionName")
    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

    /**
     * Gets the arguments.
     *
     * @return the arguments map
     */
    @JsonProperty
    public Map<String, Argument> getArguments()
    {
        return arguments;
    }
}
