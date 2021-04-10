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
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Experimental
public class SqlFunctionId
{
    private final QualifiedObjectName functionName;
    private final List<TypeSignature> argumentTypes;

    public SqlFunctionId(QualifiedObjectName functionName, List<TypeSignature> argumentTypes)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.argumentTypes = requireNonNull(argumentTypes, "argumentTypes is null");
    }

    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

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

    @JsonValue
    public String toJsonString()
    {
        return format("%s;%s", functionName.toString(), argumentTypes.stream().map(TypeSignature::toString).collect(joining(";")));
    }

    @JsonCreator
    public static SqlFunctionId parseSqlFunctionId(String signature)
    {
        String[] parts = signature.split(";");
        if (parts.length == 1) {
            return new SqlFunctionId(QualifiedObjectName.valueOf(parts[0]), emptyList());
        }
        else if (parts.length > 1) {
            QualifiedObjectName name = QualifiedObjectName.valueOf(parts[0]);
            List<TypeSignature> argumentTypes = Arrays.stream(parts, 1, parts.length)
                    .map(TypeSignature::parseTypeSignature)
                    .collect(toList());
            return new SqlFunctionId(name, argumentTypes);
        }
        else {
            throw new AssertionError(format("Invalid serialization: %s", signature));
        }
    }
}
