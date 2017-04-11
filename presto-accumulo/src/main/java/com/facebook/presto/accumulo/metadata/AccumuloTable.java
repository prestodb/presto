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
package com.facebook.presto.accumulo.metadata;

import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an Accumulo table in Presto.
 */
public class AccumuloTable
{
    private final boolean external;
    private final Integer rowIdOrdinal;
    private final String schema;
    private final String serializerClassName;
    private final Optional<String> scanAuthorizations;
    private final List<ColumnMetadata> columnsMetadata;
    private final boolean indexed;
    private final List<AccumuloColumnHandle> columns;
    private final String rowId;
    private final String table;
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public AccumuloTable(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<AccumuloColumnHandle> columns,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("external") boolean external,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations)
    {
        this.external = requireNonNull(external, "external is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns are null"));
        this.serializerClassName = requireNonNull(serializerClassName, "serializerClassName is null");
        this.scanAuthorizations = scanAuthorizations;

        boolean indexed = false;
        Optional<Integer> rowIdOrdinal = Optional.empty();

        // Extract the ColumnMetadata from the handles for faster access
        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
        for (AccumuloColumnHandle column : this.columns) {
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

    @JsonProperty
    public String getRowId()
    {
        return rowId;
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

    @JsonIgnore
    public String getIndexTableName()
    {
        return Indexer.getIndexTableName(schema, table);
    }

    @JsonIgnore
    public String getMetricsTableName()
    {
        return Indexer.getMetricsTableName(schema, table);
    }

    @JsonIgnore
    public String getFullTableName()
    {
        return getFullTableName(schema, table);
    }

    @JsonProperty
    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getScanAuthorizations()
    {
        return scanAuthorizations;
    }

    @JsonProperty
    public String getSerializerClassName()
    {
        return serializerClassName;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    @JsonProperty
    public boolean isExternal()
    {
        return external;
    }

    @JsonIgnore
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonIgnore
    public int getRowIdOrdinal()
    {
        return this.rowIdOrdinal;
    }

    @JsonIgnore
    public AccumuloRowSerializer getSerializerInstance()
    {
        try {
            return (AccumuloRowSerializer) Class.forName(serializerClassName).getConstructor().newInstance();
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new PrestoException(NOT_FOUND, "Configured serializer class not found", e);
        }
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

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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
                .add("serializerClassName", serializerClassName)
                .add("scanAuthorizations", scanAuthorizations)
                .toString();
    }
}
