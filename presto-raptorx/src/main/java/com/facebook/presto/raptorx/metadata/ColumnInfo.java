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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.raptorx.util.DatabaseUtil.getOptionalInt;
import static com.facebook.presto.raptorx.util.DatabaseUtil.optionalUtf8String;
import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8String;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnInfo
{
    private final long columnId;
    private final String columnName;
    private final Type type;
    private final Optional<String> comment;
    private final int ordinal;
    private final OptionalInt bucketOrdinal;
    private final OptionalInt sortOrdinal;

    public ColumnInfo(
            long columnId,
            String columnName,
            Type type,
            Optional<String> comment,
            int ordinal,
            OptionalInt bucketOrdinal,
            OptionalInt sortOrdinal)
    {
        this.columnId = columnId;
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.ordinal = ordinal;
        this.bucketOrdinal = requireNonNull(bucketOrdinal, "bucketOrdinal is null");
        this.sortOrdinal = requireNonNull(sortOrdinal, "sortOrdinal is null");
    }

    public long getColumnId()
    {
        return columnId;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public OptionalInt getBucketOrdinal()
    {
        return bucketOrdinal;
    }

    public OptionalInt getSortOrdinal()
    {
        return sortOrdinal;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnId", columnId)
                .toString();
    }

    public ColumnInfo withColumnName(String columnName)
    {
        return new ColumnInfo(columnId, columnName, type, comment, ordinal, bucketOrdinal, sortOrdinal);
    }

    public static class Mapper
            implements RowMapper<ColumnInfo>
    {
        private final TypeManager typeManager;

        public Mapper(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public ColumnInfo map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            String dataType = utf8String(rs.getBytes("data_type"));
            Type type = typeManager.getType(parseTypeSignature(dataType));
            verifyMetadata(type != null, "Unknown type: %s", dataType);

            return new ColumnInfo(
                    rs.getLong("column_id"),
                    utf8String(rs.getBytes("column_name")),
                    type,
                    optionalUtf8String(rs.getBytes("comment")),
                    rs.getInt("ordinal_position"),
                    getOptionalInt(rs, "bucket_ordinal_position"),
                    getOptionalInt(rs, "sort_ordinal_position"));
        }
    }
}
