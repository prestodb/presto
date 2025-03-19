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
package com.facebook.presto.kudu;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KuduColumnHandle
        implements ColumnHandle
{
    public static final String ROW_ID = "row_uuid";

    public static final KuduColumnHandle ROW_ID_HANDLE = new KuduColumnHandle(ROW_ID, -1, VarbinaryType.VARBINARY);

    private final String name;
    private final int ordinalPosition;
    private final Type type;

    @JsonCreator
    public KuduColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("type") Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.ordinalPosition = ordinalPosition;
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .build();
    }

    public boolean isVirtualRowId()
    {
        return name.equals(ROW_ID);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                name,
                ordinalPosition,
                type);
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
        KuduColumnHandle other = (KuduColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("type", type)
                .toString();
    }
}
