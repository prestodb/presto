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
package com.facebook.presto.spi;

import java.util.Objects;

import static com.facebook.presto.spi.SchemaUtil.checkNotEmpty;

public class SchemaTablePrefix
{
    /* nullable */
    private final String schemaName;
    /* nullable */
    private final String tableName;
    /* nullable */
    private final String originalSchemaName;
    /* nullable */
    private final String orignialTableName;

    public SchemaTablePrefix()
    {
        this.schemaName = null;
        this.tableName = null;
        this.originalSchemaName = null;
        this.orignialTableName = null;
    }

    public SchemaTablePrefix(String schemaName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = null;
        this.originalSchemaName = this.schemaName;
        this.orignialTableName = null;
    }

    public SchemaTablePrefix(String schemaName, String tableName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = checkNotEmpty(tableName, "tableName");
        this.originalSchemaName = schemaName;
        this.orignialTableName = tableName;
    }

    public SchemaTablePrefix(String schemaName, String tableName, String originalSchemaName, String orignialTableName)
    {
        this.schemaName = checkNotEmpty(schemaName, "schemaName");
        this.tableName = checkNotEmpty(tableName, "tableName");
        this.originalSchemaName = checkNotEmpty(originalSchemaName, "actualSchemaName");
        this.orignialTableName = checkNotEmpty(orignialTableName, "actualTableName");
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getOriginalSchemaName()
    {
        return originalSchemaName;
    }

    public String getOriginalTableName()
    {
        return orignialTableName;
    }

    public boolean matches(SchemaTableName schemaTableName)
    {
        // null schema name matches everything
        if (schemaName == null) {
            return true;
        }

        if (!(schemaName.equals(schemaTableName.getSchemaName())
                && originalSchemaName.equals(schemaTableName.getOriginalSchemaName()))) {
            return false;
        }

        return tableName == null || tableName.equals(schemaTableName.getTableName()) &&
                orignialTableName == null || orignialTableName.equals(schemaTableName.getOriginalTableName());
    }

    public SchemaTableName toSchemaTableName()
    {
        if (schemaName == null || tableName == null) {
            throw new IllegalStateException("both schemaName and tableName must be set");
        }
        return new SchemaTableName(schemaName, tableName, originalSchemaName, orignialTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, originalSchemaName, orignialTableName);
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
        final SchemaTablePrefix other = (SchemaTablePrefix) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.originalSchemaName, other.originalSchemaName) &&
                Objects.equals(this.orignialTableName, other.orignialTableName);
    }

    @Override
    public String toString()
    {
        return (schemaName == null ? "*" : schemaName) +
                '.' +
                (tableName == null ? "*" : tableName);
    }
}
