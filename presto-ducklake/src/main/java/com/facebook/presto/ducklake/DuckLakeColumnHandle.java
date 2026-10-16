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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static java.util.Objects.requireNonNull;

/**
 * A Presto column handle for one DuckLake column, reusing {@link DuckLakeColumnIdentity} the way
 * {@code IcebergColumnHandle} reuses Iceberg's {@code ColumnIdentity}. {@code defaultValue} is the
 * column's {@code initial_default} string, used for files written before the column existed.
 */
public class DuckLakeColumnHandle
        implements ColumnHandle
{
    public static final DuckLakeColumnHandle PATH_COLUMN_HANDLE = primitiveColumnHandle(
            DuckLakeMetadataColumn.PATH.getId(), DuckLakeMetadataColumn.PATH.getColumnName(), "varchar", DuckLakeMetadataColumn.PATH.getType());
    public static final ColumnMetadata PATH_COLUMN_METADATA = ColumnMetadata.builder()
            .setName(DuckLakeMetadataColumn.PATH.getColumnName())
            .setType(DuckLakeMetadataColumn.PATH.getType())
            .setHidden(true)
            .build();

    public static final DuckLakeColumnHandle ROW_ID_COLUMN_HANDLE = primitiveColumnHandle(
            DuckLakeMetadataColumn.ROW_ID.getId(), DuckLakeMetadataColumn.ROW_ID.getColumnName(), "int64", DuckLakeMetadataColumn.ROW_ID.getType());
    public static final ColumnMetadata ROW_ID_COLUMN_METADATA = ColumnMetadata.builder()
            .setName(DuckLakeMetadataColumn.ROW_ID.getColumnName())
            .setType(DuckLakeMetadataColumn.ROW_ID.getType())
            .setHidden(true)
            .build();

    /**
     * The physical row position of a row inside one Parquet data file. Hidden (excluded from
     * {@code SELECT *}) like the other metadata columns, but selectable by name; used mainly by
     * the delete filter and row-id computation (Phase 4).
     */
    public static final DuckLakeColumnHandle ROW_POSITION_COLUMN_HANDLE = primitiveColumnHandle(
            DuckLakeMetadataColumn.ROW_POSITION.getId(), DuckLakeMetadataColumn.ROW_POSITION.getColumnName(), "int64", DuckLakeMetadataColumn.ROW_POSITION.getType());
    public static final ColumnMetadata ROW_POSITION_COLUMN_METADATA = ColumnMetadata.builder()
            .setName(DuckLakeMetadataColumn.ROW_POSITION.getColumnName())
            .setType(DuckLakeMetadataColumn.ROW_POSITION.getType())
            .setHidden(true)
            .build();

    private final DuckLakeColumnIdentity columnIdentity;
    private final Type type;
    private final ColumnType columnType;
    private final Optional<String> defaultValue;

    @JsonCreator
    public DuckLakeColumnHandle(
            @JsonProperty("columnIdentity") DuckLakeColumnIdentity columnIdentity,
            @JsonProperty("type") Type type,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("defaultValue") Optional<String> defaultValue)
    {
        this.columnIdentity = requireNonNull(columnIdentity, "columnIdentity is null");
        this.type = requireNonNull(type, "type is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
    }

    @JsonProperty
    public DuckLakeColumnIdentity getColumnIdentity()
    {
        return columnIdentity;
    }

    @JsonIgnore
    public long getId()
    {
        return columnIdentity.getId();
    }

    @JsonIgnore
    public String getName()
    {
        return columnIdentity.getName();
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<String> getDefaultValue()
    {
        return defaultValue;
    }

    @JsonIgnore
    public boolean isPathColumn()
    {
        return columnIdentity.getId() == DuckLakeMetadataColumn.PATH.getId();
    }

    @JsonIgnore
    public boolean isRowIdColumn()
    {
        return columnIdentity.getId() == DuckLakeMetadataColumn.ROW_ID.getId();
    }

    @JsonIgnore
    public boolean isRowPositionColumn()
    {
        return columnIdentity.getId() == DuckLakeMetadataColumn.ROW_POSITION.getId();
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
        DuckLakeColumnHandle that = (DuckLakeColumnHandle) o;
        return columnIdentity.equals(that.columnIdentity) &&
                type.equals(that.type) &&
                columnType == that.columnType &&
                defaultValue.equals(that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIdentity, type, columnType, defaultValue);
    }

    @Override
    public String toString()
    {
        return getId() + ":" + getName() + ":" + type.getDisplayName();
    }

    /**
     * Builds a regular column handle for {@code identity}, converting its DuckLake type to a
     * Presto type through {@link TypeConverter}.
     */
    public static DuckLakeColumnHandle create(DuckLakeColumnIdentity identity, TypeManager typeManager, Optional<String> defaultValue)
    {
        return new DuckLakeColumnHandle(identity, TypeConverter.toPrestoType(identity, typeManager), REGULAR, defaultValue);
    }

    /**
     * Builds a regular column handle for a primitive column whose Presto type is already known,
     * without going through {@link TypeConverter}. Used for tests and for the hidden metadata
     * columns above.
     */
    public static DuckLakeColumnHandle primitiveColumnHandle(long id, String name, String duckLakeType, Type type)
    {
        DuckLakeColumnIdentity identity = new DuckLakeColumnIdentity(id, name, PRIMITIVE, duckLakeType, ImmutableList.of());
        return new DuckLakeColumnHandle(identity, type, REGULAR, Optional.empty());
    }

    public static boolean isMetadataColumnId(long id)
    {
        return DuckLakeMetadataColumn.isMetadataColumnId(id);
    }
}
