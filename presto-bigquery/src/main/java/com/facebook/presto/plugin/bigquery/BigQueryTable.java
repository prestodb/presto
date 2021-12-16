package com.facebook.presto.plugin.bigquery;/*
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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an BigQuery table in Presto.
 */
public class BigQueryTable
{
    private final boolean external;
    private final Integer rowIdOrdinal;
    private final String schema;
    private final Optional<String> scanAuthorizations;
    private final List<ColumnMetadata> columnsMetadata;
    private final boolean indexed;
    private final List<BigQueryColumnHandle> columns;
    private final String rowId;
    private final String table;
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public BigQueryTable(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<BigQueryColumnHandle> columns,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("external") boolean external,
            @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations)
    {
        this.external = external;
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns are null"));
        this.scanAuthorizations = scanAuthorizations;

        boolean indexed = false;
        Optional<Integer> rowIdOrdinal = Optional.empty();

        // Extract the ColumnMetadata from the handles for faster access
        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
        for (BigQueryColumnHandle column : this.columns) {
            columnMetadataBuilder.add(column.getColumnMetadata());
            indexed |= column.isIndexed();
            if (column.getName().equals(this.rowId)) {
                rowIdOrdinal = Optional.of(column.getOrdinal());
            }
        }

        if (rowIdOrdinal.isPresent()) {
            this.rowIdOrdinal = rowIdOrdinal.get();
        }
        else {
            throw new IllegalArgumentException("rowIdOrdinal is null, enable to locate rowId in given column list");
        }

        this.indexed = indexed;
        this.columnsMetadata = columnMetadataBuilder.build();
        this.schemaTableName = new SchemaTableName(this.schema, this.table);
    }

    @JsonIgnore
    public static String getFullTableName(String schema, String table)
    {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    @JsonIgnore
    public static String getFullTableName(SchemaTableName tableName)
    {
        return getFullTableName(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schema)
                .add("tableName", table)
                .add("columns", columns)
                .add("rowIdName", rowId)
                .add("external", external)
                .add("scanAuthorizations", scanAuthorizations)
                .toString();
    }
}
