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
package com.facebook.presto.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@Immutable
public class QualifiedObjectName
{
    private final String catalogName;
    private final String schemaName;
    private final String objectName;

    @JsonCreator
    public static QualifiedObjectName valueOf(String name)
    {
        if (name == null) {
            throw new NullPointerException("name is null");
        }
        String[] parts = name.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("QualifiedObjectName should have exactly 3 parts");
        }
        return new QualifiedObjectName(parts[0], parts[1], parts[2]);
    }

    public static QualifiedObjectName valueOf(CatalogSchemaName catalogSchemaName, String objectName)
    {
        return new QualifiedObjectName(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName(), objectName.toLowerCase(ENGLISH));
    }

    public static QualifiedObjectName valueOf(String catalogName, String schemaName, String objectName)
    {
        return new QualifiedObjectName(catalogName, schemaName, objectName.toLowerCase(ENGLISH));
    }

    public QualifiedObjectName(String catalogName, String schemaName, String objectName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(objectName, "objectName");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.objectName = objectName;
    }

    public CatalogSchemaName getCatalogSchemaName()
    {
        return new CatalogSchemaName(catalogName, schemaName);
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getObjectName()
    {
        return objectName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QualifiedObjectName o = (QualifiedObjectName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(objectName, o.objectName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, objectName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName + '.' + objectName;
    }

    private static void checkLowerCase(String value, String name)
    {
        requireNonNull(value, format("%s is null", name));
        if (!value.equals(value.toLowerCase(ENGLISH))) {
            throw new IllegalArgumentException(format("%s is not lowercase: %s", name, value));
        }
    }
}
