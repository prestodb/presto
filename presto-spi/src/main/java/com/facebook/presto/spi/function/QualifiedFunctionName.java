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

import com.facebook.presto.spi.CatalogSchemaName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class QualifiedFunctionName
{
    private final CatalogSchemaName functionNamespace;
    private final String name;

    private QualifiedFunctionName(CatalogSchemaName functionNamespace, String name)
    {
        this.functionNamespace = requireNonNull(functionNamespace, "functionNamespace is null");
        this.name = requireNonNull(name, "name is null");
    }

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

    public String getUnqualifiedName()
    {
        return name;
    }

    public CatalogSchemaName getFunctionNamespace()
    {
        return functionNamespace;
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
                Objects.equals(name, o.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionNamespace, name);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return functionNamespace + "." + name;
    }
}
