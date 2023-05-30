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
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.iceberg.ColumnIdentity.createColumnIdentity;
import static com.facebook.presto.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static com.facebook.presto.iceberg.IcebergColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.IcebergColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergColumnHandle
        implements ColumnHandle
{
    private final ColumnIdentity columnIdentity;
    private final Type type;
    private final Optional<String> comment;
    private final ColumnType columnType;
    private final List<Subfield> requiredSubfields;

    @JsonCreator
    public IcebergColumnHandle(
            @JsonProperty("columnIdentity") ColumnIdentity columnIdentity,
            @JsonProperty("type") Type type,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("requiredSubfields") List<Subfield> requiredSubfields)
    {
        this.columnIdentity = requireNonNull(columnIdentity, "columnIdentity is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
    }

    public IcebergColumnHandle(ColumnIdentity columnIdentity, Type type, Optional<String> comment)
    {
        this(columnIdentity, type, comment, REGULAR, ImmutableList.of());
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
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public List<Subfield> getRequiredSubfields()
    {
        return requiredSubfields;
    }

    @Override
    public ColumnHandle withRequiredSubfields(List<Subfield> subfields)
    {
        if (isPushedDownSubfield(this)) {
            // This column is already a pushed down subfield column
            return this;
        }

        return new IcebergColumnHandle(columnIdentity, type, comment, columnType, subfields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIdentity, type, comment, columnType, requiredSubfields);
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
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.requiredSubfields, other.requiredSubfields);
    }

    @Override
    public String toString()
    {
        if (requiredSubfields.isEmpty()) {
            return getId() + ":" + getName() + ":" + type.getDisplayName();
        }

        return getId() + ":" + getName() + ":" + type.getDisplayName() + ":" + columnType + ":" + requiredSubfields;
    }

    public static IcebergColumnHandle primitiveIcebergColumnHandle(int id, String name, Type type, Optional<String> comment)
    {
        return new IcebergColumnHandle(primitiveColumnIdentity(id, name), type, comment);
    }

    public static IcebergColumnHandle create(Types.NestedField column, TypeManager typeManager)
    {
        return new IcebergColumnHandle(
                createColumnIdentity(column),
                toPrestoType(column.type(), typeManager),
                Optional.ofNullable(column.doc()));
    }

    public enum ColumnType
    {
        REGULAR,
        SYNTHESIZED
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
