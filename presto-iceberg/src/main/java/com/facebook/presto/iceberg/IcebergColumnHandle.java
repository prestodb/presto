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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.types.Types;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.iceberg.ColumnIdentity.createColumnIdentity;
import static com.facebook.presto.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static java.util.Objects.requireNonNull;

public class IcebergColumnHandle
        implements ColumnHandle
{
    private final ColumnIdentity columnIdentity;
    private final Type type;
    private final Optional<String> comment;

    @JsonCreator
    public IcebergColumnHandle(
            @JsonProperty("columnIdentity") ColumnIdentity columnIdentity,
            @JsonProperty("type") Type type,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.columnIdentity = requireNonNull(columnIdentity, "columnIdentity is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
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

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIdentity, type, comment);
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
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return getId() + ":" + getName() + ":" + type.getDisplayName();
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
}
