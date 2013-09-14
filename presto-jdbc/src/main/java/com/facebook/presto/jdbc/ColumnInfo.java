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
package com.facebook.presto.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;

class ColumnInfo
{
    private final int columnType;
    private final String columnTypeName;
    private final int nullable;
    private final boolean currency;
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int columnDisplaySize;
    private final String columnLabel;
    private final String columnName;
    private final String tableName;
    private final String schemaName;
    private final String catalogName;

    public ColumnInfo(
            int columnType, String columnTypeName, int nullable, boolean currency, boolean signed, int precision, int scale,
            int columnDisplaySize, String columnLabel, String columnName, String tableName, String schemaName, String catalogName)
    {
        this.columnType = columnType;
        this.columnTypeName = checkNotNull(columnTypeName, "columnTypeName is null");
        this.nullable = nullable;
        this.currency = currency;
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.columnDisplaySize = columnDisplaySize;
        this.columnLabel = checkNotNull(columnLabel, "columnLabel is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.catalogName = checkNotNull(catalogName, "catalogName is null");
    }

    public int getColumnType()
    {
        return columnType;
    }

    public String getColumnTypeName()
    {
        return columnTypeName;
    }

    public int getNullable()
    {
        return nullable;
    }

    public boolean isCurrency()
    {
        return currency;
    }

    public boolean isSigned()
    {
        return signed;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public int getColumnDisplaySize()
    {
        return columnDisplaySize;
    }

    public String getColumnLabel()
    {
        return columnLabel;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    static class Builder
    {
        private int columnType;
        private String columnTypeName;
        private int nullable;
        private boolean currency;
        private boolean signed;
        private int precision;
        private int scale;
        private int columnDisplaySize;
        private String columnLabel;
        private String columnName;
        private String tableName;
        private String schemaName;
        private String catalogName;

        public Builder setColumnType(int columnType)
        {
            this.columnType = columnType;
            return this;
        }

        public Builder setColumnTypeName(String columnTypeName)
        {
            this.columnTypeName = columnTypeName;
            return this;
        }

        public Builder setNullable(int nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setCurrency(boolean currency)
        {
            this.currency = currency;
            return this;
        }

        public Builder setSigned(boolean signed)
        {
            this.signed = signed;
            return this;
        }

        public Builder setPrecision(int precision)
        {
            this.precision = precision;
            return this;
        }

        public Builder setScale(int scale)
        {
            this.scale = scale;
            return this;
        }

        public Builder setColumnDisplaySize(int columnDisplaySize)
        {
            this.columnDisplaySize = columnDisplaySize;
            return this;
        }

        public Builder setColumnLabel(String columnLabel)
        {
            this.columnLabel = columnLabel;
            return this;
        }

        public Builder setColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setSchemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        public ColumnInfo build()
        {
            return new ColumnInfo(
                    columnType, columnTypeName, nullable, currency, signed, precision, scale,
                    columnDisplaySize, columnLabel, columnName, tableName, schemaName, catalogName);
        }
    }
}
