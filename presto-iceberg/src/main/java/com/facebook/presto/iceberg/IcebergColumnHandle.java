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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.BaseHiveColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.iceberg.ColumnIdentity.createColumnIdentity;
import static com.facebook.presto.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.DATA_SEQUENCE_NUMBER;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.DELETE_FILE_PATH;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.FILE_PATH;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.IS_DELETED;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.MERGE_ROW_DATA;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.UPDATE_ROW_DATA;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;

public class IcebergColumnHandle
        extends BaseHiveColumnHandle
{
    public static final IcebergColumnHandle PATH_COLUMN_HANDLE = getIcebergColumnHandle(FILE_PATH);
    public static final ColumnMetadata PATH_COLUMN_METADATA = getColumnMetadata(FILE_PATH);
    public static final IcebergColumnHandle DATA_SEQUENCE_NUMBER_COLUMN_HANDLE = getIcebergColumnHandle(DATA_SEQUENCE_NUMBER);
    public static final ColumnMetadata DATA_SEQUENCE_NUMBER_COLUMN_METADATA = getColumnMetadata(DATA_SEQUENCE_NUMBER);
    public static final IcebergColumnHandle IS_DELETED_COLUMN_HANDLE = getIcebergColumnHandle(IS_DELETED);
    public static final ColumnMetadata IS_DELETED_COLUMN_METADATA = getColumnMetadata(IS_DELETED);
    public static final IcebergColumnHandle DELETE_FILE_PATH_COLUMN_HANDLE = getIcebergColumnHandle(DELETE_FILE_PATH);
    public static final ColumnMetadata DELETE_FILE_PATH_COLUMN_METADATA = getColumnMetadata(DELETE_FILE_PATH);

    private final ColumnIdentity columnIdentity;
    private final Type type;

    @JsonCreator
    public IcebergColumnHandle(
            @JsonProperty("columnIdentity") ColumnIdentity columnIdentity,
            @JsonProperty("type") Type type,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("requiredSubfields") List<Subfield> requiredSubfields)
    {
        super(columnIdentity.getName(), comment, columnType, requiredSubfields);

        this.columnIdentity = requireNonNull(columnIdentity, "columnIdentity is null");
        this.type = requireNonNull(type, "type is null");
    }

    public IcebergColumnHandle(ColumnIdentity columnIdentity, Type type, Optional<String> comment, ColumnType columnType)
    {
        this(columnIdentity, type, comment, columnType, ImmutableList.of());
    }

    @JsonProperty
    public ColumnIdentity getColumnIdentity()
    {
        return columnIdentity;
    }

    @JsonProperty
    public int getId()
    {
        return columnIdentity.getId();
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonIgnore
    public boolean isRowPositionColumn()
    {
        return columnIdentity.getId() == ROW_POSITION.fieldId();
    }

    @JsonIgnore
    public boolean isUpdateRowIdColumn()
    {
        return columnIdentity.getId() == UPDATE_ROW_DATA.getId();
    }

    @JsonIgnore
    public boolean isMergeRowIdColumn()
    {
        return columnIdentity.getId() == MERGE_ROW_DATA.getId();
    }

    @Override
    public ColumnHandle withRequiredSubfields(List<Subfield> subfields)
    {
        if (isPushedDownSubfield(this)) {
            // This column is already a pushed down subfield column
            return this;
        }

        return new IcebergColumnHandle(columnIdentity, type, getComment(), getColumnType(), subfields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIdentity, type, getComment(), getColumnType(), getRequiredSubfields());
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
        IcebergColumnHandle other = (IcebergColumnHandle) obj;
        return Objects.equals(this.columnIdentity, other.columnIdentity) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.getComment(), other.getComment()) &&
                Objects.equals(this.getColumnType(), other.getColumnType()) &&
                Objects.equals(this.getRequiredSubfields(), other.getRequiredSubfields());
    }

    @Override
    public String toString()
    {
        if (getRequiredSubfields().isEmpty()) {
            return getId() + ":" + getName() + ":" + type.getDisplayName();
        }

        return getId() + ":" + getName() + ":" + type.getDisplayName() + ":" + getColumnType() + ":" + getRequiredSubfields();
    }

    private static IcebergColumnHandle getIcebergColumnHandle(IcebergMetadataColumn metadataColumn)
    {
        return new IcebergColumnHandle(
                columnIdentity(metadataColumn),
                metadataColumn.getType(),
                Optional.empty(),
                SYNTHESIZED);
    }

    private static ColumnMetadata getColumnMetadata(IcebergMetadataColumn metadataColumn)
    {
        return ColumnMetadata.builder()
                .setName(metadataColumn.getColumnName())
                .setType(metadataColumn.getType())
                .setHidden(true)
                .build();
    }

    private static ColumnIdentity columnIdentity(IcebergMetadataColumn metadata)
    {
        return new ColumnIdentity(metadata.getId(), metadata.getColumnName(), metadata.getTypeCategory(), ImmutableList.of());
    }

    public boolean isPathColumn()
    {
        return getColumnIdentity().getId() == FILE_PATH.getId();
    }

    public boolean isDataSequenceNumberColumn()
    {
        return getColumnIdentity().getId() == DATA_SEQUENCE_NUMBER.getId();
    }

    public boolean isDeletedColumn()
    {
        return getColumnIdentity().getId() == IS_DELETED.getId();
    }

    public boolean isDeleteFilePathColumn()
    {
        return getColumnIdentity().getId() == DELETE_FILE_PATH.getId();
    }

    public static IcebergColumnHandle primitiveIcebergColumnHandle(int id, String name, Type type, Optional<String> comment)
    {
        return new IcebergColumnHandle(primitiveColumnIdentity(id, name), type, comment, REGULAR);
    }

    public static IcebergColumnHandle create(Types.NestedField column, TypeManager typeManager, ColumnType columnType)
    {
        return new IcebergColumnHandle(
                createColumnIdentity(column),
                toPrestoType(column.type(), typeManager),
                Optional.ofNullable(column.doc()),
                columnType);
    }

    public static Subfield getPushedDownSubfield(IcebergColumnHandle column)
    {
        checkArgument(isPushedDownSubfield(column), format("not a valid pushed down subfield: type=%s, subfields=%s", column.getColumnType(), column.getRequiredSubfields()));
        return getOnlyElement(column.getRequiredSubfields());
    }

    public static boolean isPushedDownSubfield(IcebergColumnHandle column)
    {
        return column.getColumnType() == SYNTHESIZED && column.getRequiredSubfields().size() == 1;
    }

    public static IcebergColumnHandle getSynthesizedIcebergColumnHandle(String pushdownColumnName, Type pushdownColumnType, Subfield requiredSubfields)
    {
        return getSynthesizedIcebergColumnHandle(pushdownColumnName, pushdownColumnType, ImmutableList.of(requiredSubfields));
    }

    public static IcebergColumnHandle getSynthesizedIcebergColumnHandle(String pushdownColumnName, Type pushdownColumnType, List<Subfield> requiredSubfields)
    {
        return new IcebergColumnHandle(
                primitiveColumnIdentity(-1, pushdownColumnName),
                pushdownColumnType,
                Optional.of("nested column pushdown"),
                SYNTHESIZED,
                requiredSubfields);
    }
}
