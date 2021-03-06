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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.spi.api.Experimental;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Experimental
public class SqlFunctionHandle
        implements FunctionHandle
{
    private final SqlFunctionId functionId;
    private final String version;

    @JsonCreator
    public SqlFunctionHandle(
            @JsonProperty("functionId") SqlFunctionId functionId,
            @JsonProperty("version") String version)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.version = version;
    }

    @JsonProperty
    public SqlFunctionId getFunctionId()
    {
        return functionId;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @Override
    public CatalogSchemaName getCatalogSchemaName()
    {
        return functionId.getFunctionName().getCatalogSchemaName();
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
        SqlFunctionHandle o = (SqlFunctionHandle) obj;
        return Objects.equals(functionId, o.functionId)
                && Objects.equals(version, o.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionId, version);
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s", functionId, version);
    }

    public static class Resolver
            implements FunctionHandleResolver
    {
        @Override
        public Class<? extends FunctionHandle> getFunctionHandleClass()
        {
            return SqlFunctionHandle.class;
        }
    }
}
