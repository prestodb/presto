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
package com.facebook.presto.raptorx;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class RaptorColumnHandle
        implements ColumnHandle
{
    public static final RaptorColumnHandle CHUNK_ROW_ID_HANDLE = new RaptorColumnHandle(-1, "$chunk_row_id", BIGINT, Optional.empty());
    public static final RaptorColumnHandle CHUNK_ID_HANDLE = new RaptorColumnHandle(-2, "$chunk_id", BIGINT, Optional.empty());
    public static final RaptorColumnHandle BUCKET_NUMBER_HANDLE = new RaptorColumnHandle(-3, "$bucket_number", INTEGER, Optional.empty());

    private final long columnId;
    private final String columnName;
    private final Type columnType;
    private final Optional<String> comment;

    @JsonCreator
    public RaptorColumnHandle(
            @JsonProperty("columnId") long columnId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.columnId = columnId;
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
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

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
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
        RaptorColumnHandle other = (RaptorColumnHandle) obj;
        return Objects.equals(this.columnId, other.columnId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnId);
    }

    @Override
    public String toString()
    {
        return columnId + ":" + columnName + ":" + columnType;
    }

    public boolean isHidden()
    {
        return isHiddenColumn(columnId);
    }

    public static boolean isHiddenColumn(long columnId)
    {
        return columnId < 0;
    }

    public static boolean isChunkRowIdColumn(long columnId)
    {
        return columnId == CHUNK_ROW_ID_HANDLE.getColumnId();
    }

    public static boolean isChunkIdColumn(long columnId)
    {
        return columnId == CHUNK_ID_HANDLE.getColumnId();
    }

    public static boolean isBucketNumberColumn(long columnId)
    {
        return columnId == BUCKET_NUMBER_HANDLE.getColumnId();
    }
}
