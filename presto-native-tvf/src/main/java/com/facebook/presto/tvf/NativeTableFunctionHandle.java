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

import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class NativeTableFunctionHandle
        implements ConnectorTableFunctionHandle
{
    private final String functionName;
    private final String serializedTableFunctionHandle;

    @JsonCreator
    public NativeTableFunctionHandle(
            @JsonProperty("serializedTableFunctionHandle") String serializedTableFunctionHandle,
            @JsonProperty("functionName") String functionName)
    {
        this.serializedTableFunctionHandle = requireNonNull(serializedTableFunctionHandle, "serializedTableFunctionHandle is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    @JsonProperty
    public String getSerializedTableFunctionHandle()
    {
        return serializedTableFunctionHandle;
    }

    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    public static class Resolver
            implements TableFunctionHandleResolver
    {
        @Override
        public Set<Class<? extends ConnectorTableFunctionHandle>> getTableFunctionHandleClasses()
        {
            return ImmutableSet.of(NativeTableFunctionHandle.class);
        }
    }
}
