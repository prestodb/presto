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
package com.facebook.presto.common.function;

import com.facebook.presto.common.CatalogSchemaName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedFunctionName
{
    // TODO: Move out this class. Ideally this class should not be in presto-common module.

    private final CatalogSchemaName functionNamespace;
    private final String functionName;

    private QualifiedFunctionName(CatalogSchemaName functionNamespace, String functionName)
    {
        this.functionNamespace = requireNonNull(functionNamespace, "functionNamespace is null");
        this.functionName = requireNonNull(functionName, "name is null").toLowerCase(ENGLISH);
    }

    // This function should only be used for JSON deserialization. Do not use it directly.
    @JsonCreator
    public static QualifiedFunctionName of(String dottedName)
    {
        String[] parts = dottedName.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("QualifiedFunctionName should have exactly 3 parts");
        }
        return of(new CatalogSchemaName(parts[0], parts[1]), parts[2]);
    }

    public static QualifiedFunctionName of(CatalogSchemaName functionNamespace, String name)
    {
        return new QualifiedFunctionName(functionNamespace, name);
    }

    public CatalogSchemaName getFunctionNamespace()
    {
        return functionNamespace;
    }

    // TODO: Examine all callers to limit the usage of the method
    public String getFunctionName()
    {
        return functionName;
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
        QualifiedFunctionName o = (QualifiedFunctionName) obj;
        return Objects.equals(functionNamespace, o.functionNamespace) &&
                Objects.equals(functionName, o.functionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionNamespace, functionName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return functionNamespace + "." + functionName;
    }
}
