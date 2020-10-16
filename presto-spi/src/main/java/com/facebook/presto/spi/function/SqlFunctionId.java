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

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

@Experimental
public class SqlFunctionId
{
    private final QualifiedObjectName functionName;
    private final List<TypeSignature> argumentTypes;

    @JsonCreator
    public SqlFunctionId(
            @JsonProperty("functionName") QualifiedObjectName functionName,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
    }

    @JsonProperty
    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    public String getId()
    {
        return toString();
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
        SqlFunctionId o = (SqlFunctionId) obj;
        return Objects.equals(functionName, o.functionName)
                && Objects.equals(argumentTypes, o.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, argumentTypes);
    }

    @Override
    public String toString()
    {
        String arguments = argumentTypes.stream()
                .map(Object::toString)
                .collect(joining(", "));
        return format("%s(%s)", functionName, arguments);
    }
}
