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
package com.facebook.presto.raptor.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.OptionalInt;

import static com.facebook.presto.raptor.util.DatabaseUtil.getOptionalInt;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Table
{
    private final long tableId;
    private final OptionalInt bucketCount;

    public Table(long tableId, OptionalInt bucketCount)
    {
        this.tableId = tableId;
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, bucketCount);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Table o = (Table) obj;
        return tableId == o.tableId &&
                Objects.equals(bucketCount, o.bucketCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("bucketCount", bucketCount.isPresent() ? bucketCount.getAsInt() : null)
                .omitNullValues()
                .toString();
    }

    public static class TableMapper
            implements ResultSetMapper<Table>
    {
        @Override
        public Table map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new Table(
                    r.getLong("table_id"),
                    getOptionalInt(r, "bucket_count"));
        }
    }
}
