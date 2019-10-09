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
package com.facebook.presto.sqlfunction;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class SqlInvokedRegularFunctionHandle
        implements FunctionHandle
{
    private final FullyQualifiedName name;
    private final List<TypeSignature> argumentTypes;
    private final long version;

    @JsonCreator
    public SqlInvokedRegularFunctionHandle(
            @JsonProperty("name") FullyQualifiedName name,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("version") long version)
    {
        this.name = requireNonNull(name, "name is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
        this.version = version;
    }

    @JsonProperty
    public FullyQualifiedName getName()
    {
        return name;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @Override
    public FullyQualifiedName.Prefix getFunctionNamespace()
    {
        return name.getPrefix();
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
        SqlInvokedRegularFunctionHandle o = (SqlInvokedRegularFunctionHandle) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(argumentTypes, o.argumentTypes)
                && Objects.equals(version, o.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, argumentTypes, version);
    }

    @Override
    public String toString()
    {
        String arguments = argumentTypes.stream()
                .map(Object::toString)
                .collect(joining(", "));
        return String.format("%s(%s):%s", name, arguments, version);
    }
}
