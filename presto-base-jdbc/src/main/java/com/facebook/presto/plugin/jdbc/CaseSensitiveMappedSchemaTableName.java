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
package com.facebook.presto.plugin.jdbc;

import static java.util.Locale.ENGLISH;

public class CaseSensitiveMappedSchemaTableName
{
    private final String schemaName;
    private final String schemaNameLower;
    private final String tableName;
    private final String tableNameLower;

    public CaseSensitiveMappedSchemaTableName(String schemaName, String tableName)
    {
        if (schemaName == null) {
            throw new NullPointerException("schemaName is null");
        }
        if (schemaName.isEmpty()) {
            throw new IllegalArgumentException("schemaName is empty");
        }
        this.schemaName = schemaName;
        this.schemaNameLower = schemaName.toLowerCase(ENGLISH);

        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("tableName is empty");
        }
        this.tableName = tableName;
        this.tableNameLower = tableName.toLowerCase(ENGLISH);
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getSchemaNameLower()
    {
        return schemaNameLower;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getTableNameLower()
    {
        return tableNameLower;
    }
}
