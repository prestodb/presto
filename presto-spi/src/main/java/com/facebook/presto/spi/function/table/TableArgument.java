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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.common.type.RowType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.function.table.ConnectorTableFunction.checkNotNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * This class represents the table argument passed to a Table Function.
 * <p>
 * This representation should be considered experimental. Eventually, {@link ConnectorExpression}
 * should be extended to include this kind of argument.
 */
public class TableArgument
        extends Argument
{
    private final Optional<QualifiedName> name;
    private final RowType rowType;
    private final List<String> partitionBy;
    private final List<SortItem> orderBy;
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final boolean passThroughColumns;

    @JsonCreator
    public TableArgument(
            @JsonProperty("name") Optional<QualifiedName> name,
            @JsonProperty("rowType") RowType rowType,
            @JsonProperty("partitionBy") List<String> partitionBy,
            @JsonProperty("orderBy") List<SortItem> orderBy,
            @JsonProperty("rowSemantics") boolean rowSemantics,
            @JsonProperty("pruneWhenEmpty") boolean pruneWhenEmpty,
            @JsonProperty("passThroughColumns") boolean passThroughColumns)
    {
        this.name = requireNonNull(name, "name is null");
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
    }

    @JsonProperty
    public Optional<QualifiedName> getName()
    {
        return name;
    }

    @JsonProperty
    public RowType getRowType()
    {
        return rowType;
    }

    @JsonProperty
    public List<String> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public boolean isRowSemantics()
    {
        return rowSemantics;
    }

    @JsonProperty
    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

    @JsonProperty
    public boolean isPassThroughColumns()
    {
        return passThroughColumns;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<QualifiedName> name;
        private RowType rowType;
        private List<String> partitionBy = Collections.emptyList();
        private List<SortItem> orderBy = Collections.emptyList();
        private boolean rowSemantics;
        private boolean pruneWhenEmpty;
        private boolean passThroughColumns;

        private Builder() {}

        public Builder name(Optional<QualifiedName> name)
        {
            this.name = name;
            return this;
        }

        public Builder rowType(RowType rowType)
        {
            this.rowType = rowType;
            return this;
        }

        public Builder partitionBy(List<String> partitionBy)
        {
            this.partitionBy = partitionBy;
            return this;
        }

        public Builder orderBy(List<SortItem> orderBy)
        {
            this.orderBy = orderBy;
            return this;
        }

        public Builder rowSemantics(boolean rowSemantics)
        {
            this.rowSemantics = rowSemantics;
            return this;
        }

        public Builder pruneWhenEmpty(boolean pruneWhenEmpty)
        {
            this.pruneWhenEmpty = pruneWhenEmpty;
            return this;
        }

        public Builder passThroughColumns(boolean passThroughColumns)
        {
            this.passThroughColumns = passThroughColumns;
            return this;
        }

        public TableArgument build()
        {
            return new TableArgument(name, rowType, partitionBy, orderBy, rowSemantics, pruneWhenEmpty, passThroughColumns);
        }
    }

    public static class QualifiedName
    {
        private final String catalogName;
        private final String schemaName;
        private final String tableName;

        @JsonCreator
        public QualifiedName(
                @JsonProperty("catalogName") String catalogName,
                @JsonProperty("schemaName") String schemaName,
                @JsonProperty("tableName") String tableName)
        {
            this.catalogName = checkNotNullOrEmpty(catalogName, "catalogName");
            this.schemaName = checkNotNullOrEmpty(schemaName, "schemaName");
            this.tableName = checkNotNullOrEmpty(tableName, "tableName");
        }

        @JsonProperty
        public String getCatalogName()
        {
            return catalogName;
        }

        @JsonProperty
        public String getSchemaName()
        {
            return schemaName;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }
    }

    public static class SortItem
    {
        private final String column;
        private final boolean ascending;
        private final boolean nullsLast;

        @JsonCreator
        public SortItem(
                @JsonProperty("column") String column,
                @JsonProperty("ascending") boolean ascending,
                @JsonProperty("nullsFirst") boolean nullsFirst)
        {
            this.column = checkNotNullOrEmpty(column, "ordering column");
            this.ascending = ascending;
            this.nullsLast = nullsFirst;
        }

        @JsonProperty
        public String getColumn()
        {
            return column;
        }

        @JsonProperty
        public boolean isAscending()
        {
            return ascending;
        }

        @JsonProperty
        public boolean isNullsLast()
        {
            return nullsLast;
        }
    }
}
