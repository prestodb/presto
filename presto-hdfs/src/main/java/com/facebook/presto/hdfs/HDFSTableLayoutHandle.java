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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.function.Function;
import com.facebook.presto.hdfs.function.Function0;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.IntegerType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSTableLayoutHandle
implements ConnectorTableLayoutHandle
{
    private final HDFSTableHandle table;
    private final HDFSColumnHandle fiberColumn;
    private final HDFSColumnHandle timestampColumn;
    private final Function fiberFunction;
    private final StorageFormat storageFormat;
    private Optional<TupleDomain<ColumnHandle>> predicates;

    public HDFSTableLayoutHandle(HDFSTableHandle table)
    {
        this(table,
                new HDFSColumnHandle("null", IntegerType.INTEGER, "", HDFSColumnHandle.ColumnType.REGULAR, ""),
                new HDFSColumnHandle("null", IntegerType.INTEGER, "", HDFSColumnHandle.ColumnType.REGULAR, ""),
                new Function0(80),
                StorageFormat.PARQUET,
                Optional.empty());
    }

    @JsonCreator
    public HDFSTableLayoutHandle(
            @JsonProperty("table") HDFSTableHandle table,
            @JsonProperty("fiberColumn") HDFSColumnHandle fiberColumn,
            @JsonProperty("timestampColumn") HDFSColumnHandle timestampColumn,
            @JsonProperty("fiberFunction") Function fiberFunction,
            @JsonProperty("storageFormat") StorageFormat storageFormat,
            @JsonProperty("predicates") Optional<TupleDomain<ColumnHandle>> predicates)
    {
        this.table = requireNonNull(table, "table is null");
        this.fiberColumn = requireNonNull(fiberColumn, "fiberColumn is null");
        this.timestampColumn = requireNonNull(timestampColumn, "timestampColumn is null");
        this.fiberFunction = requireNonNull(fiberFunction, "fiberFunc is null");
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.predicates = requireNonNull(predicates, "predicates is null");
    }

    @JsonProperty
    public HDFSTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(table.getSchemaName(), table.getTableName());
    }

    @JsonProperty
    public HDFSColumnHandle getFiberColumn()
    {
        return fiberColumn;
    }

    @JsonProperty
    public HDFSColumnHandle getTimestampColumn()
    {
        return timestampColumn;
    }

    @JsonProperty
    public Function getFiberFunction()
    {
        return fiberFunction;
    }

    @JsonProperty
    public StorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    public void setPredicates(Optional<TupleDomain<ColumnHandle>> predicates)
    {
        this.predicates = predicates;
    }

    @JsonProperty
    public Optional<TupleDomain<ColumnHandle>> getPredicates()
    {
        return predicates;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, fiberColumn, timestampColumn, fiberFunction, storageFormat);
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

        HDFSTableLayoutHandle other = (HDFSTableLayoutHandle) obj;
        return Objects.equals(table, other.table) &&
                Objects.equals(fiberColumn, other.fiberColumn) &&
                Objects.equals(timestampColumn, other.timestampColumn) &&
                Objects.equals(fiberFunction, other.fiberFunction) &&
                Objects.equals(storageFormat, other.storageFormat);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("fiber column", fiberColumn)
                .add("timestamp column", timestampColumn)
                .add("fiber function", fiberFunction)
                .add("storage format", storageFormat)
                .toString();
    }
}
