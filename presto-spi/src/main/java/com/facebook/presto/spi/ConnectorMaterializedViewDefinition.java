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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class ConnectorMaterializedViewDefinition
{
    private final String originalSql;
    private final String schema;
    private final String table;
    private final List<SchemaTableName> baseTables;
    private final Optional<String> owner;
    private final List<ColumnMapping> columnMappings;
    private final Optional<List<String>> validRefreshColumns;

    @JsonCreator
    public ConnectorMaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("baseTables") List<SchemaTableName> baseTables,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("columnMapping") List<ColumnMapping> columnMappings,
            @JsonProperty("validRefreshColumns") Optional<List<String>> validRefreshColumns)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.baseTables = unmodifiableList(new ArrayList<>(requireNonNull(baseTables, "baseTables is null")));
        this.owner = requireNonNull(owner, "owner is null");
        this.columnMappings = unmodifiableList(new ArrayList<>(requireNonNull(columnMappings, "columnMappings is null")));
        this.validRefreshColumns = requireNonNull(validRefreshColumns, "validRefreshColumns is null").map(columns -> unmodifiableList(new ArrayList<>(columns)));
    }

    @JsonIgnore
    public ConnectorMaterializedViewDefinition(
            String originalSql,
            String schema,
            String table,
            List<SchemaTableName> baseTables,
            Optional<String> owner,
            Map<String, Map<SchemaTableName, String>> originalColumnMapping,
            Optional<List<String>> validRefreshColumns)
    {
        this(
                originalSql,
                schema,
                table,
                baseTables,
                owner,
                convertFromMapToColumnMappings(requireNonNull(originalColumnMapping, "originalColumnMapping is null"), new SchemaTableName(schema, table)),
                validRefreshColumns);
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public List<SchemaTableName> getBaseTables()
    {
        return baseTables;
    }

    @JsonProperty
    public Optional<String> getOwner()
    {
        return owner;
    }

    @JsonProperty
    public List<ColumnMapping> getColumnMappings()
    {
        return columnMappings;
    }

    @JsonProperty
    public Optional<List<String>> getValidRefreshColumns()
    {
        return validRefreshColumns;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorMaterializedViewDefinition{");
        sb.append("originalSql=").append(originalSql);
        sb.append(",schema=").append(schema);
        sb.append(",table=").append(table);
        sb.append(",baseTables=").append(baseTables);
        sb.append(",owner=").append(owner.orElse(null));
        sb.append(",columnMappings=").append(columnMappings);
        sb.append(",validRefreshColumns=").append(validRefreshColumns.orElse(null));
        sb.append("}");
        return sb.toString();
    }

    @JsonIgnore
    public Map<String, Map<SchemaTableName, String>> getColumnMappingsAsMap()
    {
        return columnMappings.stream()
                .collect(toMap(
                        mapping -> mapping.getViewColumn().getColumnName(),
                        mapping -> mapping.getBaseTableColumns().stream().collect(toMap(TableColumn::getTableName, TableColumn::getColumnName))));
    }

    @JsonIgnore
    private static List<ColumnMapping> convertFromMapToColumnMappings(Map<String, Map<SchemaTableName, String>> originalColumnMappings, SchemaTableName sourceTable)
    {
        List<ColumnMapping> columnMappingList = new ArrayList<>();

        for (String sourceColumn : originalColumnMappings.keySet()) {
            TableColumn viewColumn = new TableColumn(sourceTable, sourceColumn);

            List<TableColumn> baseTableColumns = new ArrayList<>();
            for (SchemaTableName baseTable : originalColumnMappings.get(sourceColumn).keySet()) {
                baseTableColumns.add(new TableColumn(baseTable, originalColumnMappings.get(sourceColumn).get(baseTable)));
            }

            columnMappingList.add(new ColumnMapping(viewColumn, unmodifiableList(baseTableColumns)));
        }

        return unmodifiableList(columnMappingList);
    }

    public static final class ColumnMapping
    {
        private final TableColumn viewColumn;
        private final List<TableColumn> baseTableColumns;

        @JsonCreator
        public ColumnMapping(
                @JsonProperty("viewColumn") TableColumn viewColumn,
                @JsonProperty("baseTableColumns") List<TableColumn> baseTableColumns)
        {
            this.viewColumn = requireNonNull(viewColumn, "viewColumn is null");
            this.baseTableColumns = unmodifiableList(new ArrayList<>(requireNonNull(baseTableColumns, "baseTableColumns is null")));
        }

        @JsonProperty
        public TableColumn getViewColumn()
        {
            return viewColumn;
        }

        @JsonProperty
        public List<TableColumn> getBaseTableColumns()
        {
            return baseTableColumns;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("ColumnMapping{");
            sb.append("viewColumn=").append(viewColumn);
            sb.append(",baseTableColumns=").append(baseTableColumns);
            sb.append("}");
            return sb.toString();
        }
    }

    public static final class TableColumn
    {
        private final SchemaTableName tableName;
        private final String columnName;

        @JsonCreator
        public TableColumn(
                @JsonProperty("tableName") SchemaTableName tableName,
                @JsonProperty("columnName") String columnName)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        @JsonProperty
        public SchemaTableName getTableName()
        {
            return tableName;
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, columnName);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TableColumn that = (TableColumn) o;
            return Objects.equals(this.columnName, that.columnName) &&
                    Objects.equals(this.tableName, that.tableName);
        }

        @Override
        public String toString()
        {
            return tableName + ":" + columnName;
        }
    }
}
