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
package io.prestosql.plugin.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ThriftColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final String comment;
    private final boolean hidden;

    @JsonCreator
    public ThriftColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("comment") @Nullable String comment,
            @JsonProperty("hidden") boolean hidden)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = comment;
        this.hidden = hidden;
    }

    public ThriftColumnHandle(ColumnMetadata columnMetadata)
    {
        this(columnMetadata.getName(), columnMetadata.getType(), columnMetadata.getComment(), columnMetadata.isHidden());
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @Nullable
    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, comment, hidden);
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
        ThriftColumnHandle other = (ThriftColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.comment, other.comment) &&
                this.hidden == other.hidden;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, comment, hidden);
    }

    @Override
    public String toString()
    {
        return columnName + ":" + columnType;
    }
}
