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

import com.facebook.presto.jdbc.TypeInfo.Nullable;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;

import static java.util.Objects.requireNonNull;

class ColumnInfo
{
    private final TypeInfo typeInfo;
    private final String columnLabel;
    private final String columnName;
    private final String tableName;
    private final String schemaName;
    private final String catalogName;

    public ColumnInfo(
            TypeInfo typeInfo,
            String columnLabel,
            String columnName,
            String tableName,
            String schemaName,
            String catalogName)
    {
        this.typeInfo = requireNonNull(typeInfo, "typeInfo is null");
        this.columnLabel = requireNonNull(columnLabel, "columnLabel is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    public int getColumnType()
    {
        return typeInfo.getSqlType();
    }

    public List<Integer> getColumnParameterTypes()
    {
        return typeInfo.getParameterSqlTypes();
    }

    public TypeSignature getColumnTypeSignature()
    {
        return typeInfo.getTypeSignature();
    }

    public String getColumnTypeName()
    {
        return typeInfo.getTypeSignature().toString();
    }

    public Nullable getNullable()
    {
        return typeInfo.getNullable();
    }

    public boolean isCurrency()
    {
        return typeInfo.isCurrency();
    }

    public boolean isSigned()
    {
        return typeInfo.isSigned();
    }

    public int getPrecision()
    {
        return typeInfo.getPrecision();
    }

    public int getScale()
    {
        return typeInfo.getScale();
    }

    public int getColumnDisplaySize()
    {
        return typeInfo.getDisplaySize();
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
        private TypeInfo.Builder typeInfoBuilder = new TypeInfo.Builder();
        private String columnLabel;
        private String columnName;
        private String tableName;
        private String schemaName;
        private String catalogName;

        public Builder setTypeSignature(TypeSignature signature)
        {
            this.typeInfoBuilder.setTypeSignature(signature);
            return this;
        }

        public Builder setNullable(Nullable nullable)
        {
            this.typeInfoBuilder.setNullable(nullable);
            return this;
        }

        public Builder setCurrency(boolean currency)
        {
            this.typeInfoBuilder.setCurrency(currency);
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
                    typeInfoBuilder.build(),
                    columnLabel,
                    columnName,
                    tableName,
                    schemaName,
                    catalogName);
        }
    }
}
