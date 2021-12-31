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
    private final List<SchemaTableName> baseTablesOnOuterJoinSide;
    private final Optional<List<String>> validRefreshColumns;

    @JsonCreator
    public ConnectorMaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("baseTables") List<SchemaTableName> baseTables,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("columnMapping") List<ColumnMapping> columnMappings,
            @JsonProperty("baseTablesOnOuterJoinSide") List<SchemaTableName> baseTablesOnOuterJoinSide,
            @JsonProperty("validRefreshColumns") Optional<List<String>> validRefreshColumns)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.baseTables = unmodifiableList(new ArrayList<>(requireNonNull(baseTables, "baseTables is null")));
        this.owner = requireNonNull(owner, "owner is null");
        this.columnMappings = unmodifiableList(new ArrayList<>(requireNonNull(columnMappings, "columnMappings is null")));
        this.baseTablesOnOuterJoinSide = unmodifiableList(new ArrayList<>(requireNonNull(baseTablesOnOuterJoinSide, "baseTablesOnOuterJoinSide is null")));
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
            Map<String, Map<SchemaTableName, String>> nonNullColumnMappings,
            List<SchemaTableName> baseTablesOnOuterJoinSide,
            Optional<List<String>> validRefreshColumns)
    {
        this(
                originalSql,
                schema,
                table,
                baseTables,
                owner,
                convertFromMapToColumnMappings(
                        requireNonNull(originalColumnMapping, "originalColumnMapping is null"),
                        requireNonNull(nonNullColumnMappings, "nonNullColumnMappings is null"),
                        new SchemaTableName(schema, table)),
                baseTablesOnOuterJoinSide,
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
    public List<SchemaTableName> getBaseTablesOnOuterJoinSide()
    {
        return baseTablesOnOuterJoinSide;
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
        sb.append(",baseTablesOnOuterJoinSide=").append(baseTablesOnOuterJoinSide);
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
    public Map<String, Map<SchemaTableName, String>> getDirectColumnMappingsAsMap()
    {
        return columnMappings.stream()
                .collect(toMap(
                        mapping -> mapping.getViewColumn().getColumnName(),
                        mapping -> mapping.getBaseTableColumns().stream().filter(col -> col.isDirectMapped().orElse(true)).collect(toMap(TableColumn::getTableName, TableColumn::getColumnName))));
    }

    @JsonIgnore
    private static List<ColumnMapping> convertFromMapToColumnMappings(Map<String, Map<SchemaTableName, String>> originalColumnMappings, Map<String, Map<SchemaTableName, String>> directColumnMappings, SchemaTableName sourceTable)
    {
        List<ColumnMapping> columnMappingList = new ArrayList<>();

        for (String sourceColumn : originalColumnMappings.keySet()) {
            TableColumn viewColumn = new TableColumn(sourceTable, sourceColumn, true);
            Map<SchemaTableName, String> directMappings = directColumnMappings.get(sourceColumn);

            List<TableColumn> baseTableColumns = new ArrayList<>();
            for (SchemaTableName baseTable : originalColumnMappings.get(sourceColumn).keySet()) {
                String column = originalColumnMappings.get(sourceColumn).get(baseTable);
                boolean isDirectMapped = directMappings != null && (directMappings.containsKey(baseTable) && directMappings.get(baseTable) == column);
                baseTableColumns.add(new TableColumn(baseTable, column, isDirectMapped));
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
        // This signifies whether the mapping is direct or not.
        // Mapping is always direct in inner join case. In the outer join case, only the mapping from a column to its source column in the join input table is direct.
        // For e.g. in case of SELECT t1_a as t1.a, t2_a as t2.a FROM t1 LEFT JOIN t2 ON t1.a = t2.a
        // t1_a -> t1.a is direct mapped
        // t1_a -> t2.a is NOT direct mapped(as t1,t2 are in outer join)
        // t2_a -> t2.a is direct mapped(value can become null but column mapping is not altered)
        // t2_a -> t1.a is NOT direct mapped(as t1,t2 are in outer join)
        private final Optional<Boolean> isDirectMapped;

        @JsonCreator
        public TableColumn(
                @JsonProperty("tableName") SchemaTableName tableName,
                @JsonProperty("columnName") String columnName,
                @JsonProperty("isDirectMapped") Optional<Boolean> isDirectMapped)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.isDirectMapped = requireNonNull(isDirectMapped, "isDirectMapped is null");
        }

        public TableColumn(
                SchemaTableName tableName,
                String columnName,
                boolean isDirectMapped)
        {
            this(tableName, columnName, Optional.of(isDirectMapped));
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

        @JsonProperty
        public Optional<Boolean> isDirectMapped()
        {
            return isDirectMapped;
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
                    Objects.equals(this.tableName, that.tableName) &&
                    Objects.equals(this.isDirectMapped, that.isDirectMapped);
        }

        @Override
        public String toString()
        {
            return tableName + ":" + columnName + (isDirectMapped.orElse(true) ? "" : "isDirectMapped:false");
        }
    }
}
