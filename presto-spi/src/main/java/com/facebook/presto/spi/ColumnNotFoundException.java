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

public class ColumnNotFoundException
        extends NotFoundException
{
    private final SchemaTableName tableName;
    private final String columnName;

    public ColumnNotFoundException(SchemaTableName tableName, String columnName)
    {
        this(tableName, columnName, "Column " + columnName + " not found in table " + tableName);
    }

    public ColumnNotFoundException(SchemaTableName tableName, String columnName, String message)
    {
        super(message);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        if (columnName == null) {
            throw new NullPointerException("columnName is null");
        }
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public ColumnNotFoundException(SchemaTableName tableName, String columnName, Throwable cause)
    {
        this(tableName, columnName, "Table " + tableName + " not found", cause);
    }

    public ColumnNotFoundException(SchemaTableName tableName, String columnName, String message, Throwable cause)
    {
        super(message, cause);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        if (columnName == null) {
            throw new NullPointerException("columnName is null");
        }
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getColumnName()
    {
        return columnName;
    }
}
