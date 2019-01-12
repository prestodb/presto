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
package io.prestosql.plugin.raptor.legacy.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalInt;

import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.getOptionalInt;
import static java.util.Objects.requireNonNull;

public class ColumnMetadataRow
{
    private final long tableId;
    private final long columnId;
    private final String columnName;
    private final OptionalInt sortOrdinalPosition;
    private final OptionalInt bucketOrdinalPosition;

    public ColumnMetadataRow(long tableId, long columnId, String columnName, OptionalInt sortOrdinalPosition, OptionalInt bucketOrdinalPosition)
    {
        this.tableId = tableId;
        this.columnId = columnId;
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.sortOrdinalPosition = requireNonNull(sortOrdinalPosition, "sortOrdinalPosition is null");
        this.bucketOrdinalPosition = requireNonNull(bucketOrdinalPosition, "bucketOrdinalPosition is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getColumnId()
    {
        return columnId;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public OptionalInt getSortOrdinalPosition()
    {
        return sortOrdinalPosition;
    }

    public OptionalInt getBucketOrdinalPosition()
    {
        return bucketOrdinalPosition;
    }

    public static class Mapper
            implements ResultSetMapper<ColumnMetadataRow>
    {
        @Override
        public ColumnMetadataRow map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ColumnMetadataRow(
                    rs.getLong("table_id"),
                    rs.getLong("column_id"),
                    rs.getString("column_name"),
                    getOptionalInt(rs, "sort_ordinal_position"),
                    getOptionalInt(rs, "bucket_ordinal_position"));
        }
    }
}
