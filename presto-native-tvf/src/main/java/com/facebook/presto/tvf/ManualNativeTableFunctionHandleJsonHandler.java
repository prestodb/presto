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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Manual JSON handler for native table function handle.
 * This is a temporary solution to manually convert a
 * NativeTableFunctionHandle JSON.
 */
public final class ManualNativeTableFunctionHandleJsonHandler
{
    private final String type;
    private final String serializedTableFunctionHandle;
    private final QualifiedObjectName functionName;

    /**
     * Constructs a manual native table function handle JSON handler.
     *
     * @param type the type
     * @param serializedTableFunctionHandle the serialized handle
     * @param functionName the function name
     */
    @JsonCreator
    public ManualNativeTableFunctionHandleJsonHandler(
            @JsonProperty("@type") final String type,
            @JsonProperty("serializedTableFunctionHandle")
            final String serializedTableFunctionHandle,
            @JsonProperty("functionName")
            final QualifiedObjectName functionName)
    {
        this.type = requireNonNull(type, "type is null");
        this.serializedTableFunctionHandle = requireNonNull(
                serializedTableFunctionHandle,
                "serializedTableFunctionHandle is null");
        this.functionName = requireNonNull(functionName,
                "functionName is null");
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    @JsonProperty("@type")
    public String getType()
    {
        return type;
    }

    /**
     * Gets the serialized table function handle.
     *
     * @return the serialized table function handle
     */
    @JsonProperty
    public String getSerializedTableFunctionHandle()
    {
        return serializedTableFunctionHandle;
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
}
