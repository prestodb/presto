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

import static java.util.Objects.requireNonNull;

public class TableStatsRow
{
    private final String schemaName;
    private final String tableName;
    private final long createTime;
    private final long updateTime;
    private final long tableVersion;
    private final long shardCount;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;

    public TableStatsRow(
            String schemaName,
            String tableName,
            long createTime,
            long updateTime,
            long tableVersion,
            long shardCount,
            long rowCount,
            long compressedSize,
            long uncompressedSize)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.tableVersion = tableVersion;
        this.shardCount = shardCount;
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public long getUpdateTime()
    {
        return updateTime;
    }

    public long getTableVersion()
    {
        return tableVersion;
    }

    public long getShardCount()
    {
        return shardCount;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getCompressedSize()
    {
        return compressedSize;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    public static class Mapper
            implements ResultSetMapper<TableStatsRow>
    {
        @Override
        public TableStatsRow map(int index, ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new TableStatsRow(
                    rs.getString("schema_name"),
                    rs.getString("table_name"),
                    rs.getLong("create_time"),
                    rs.getLong("update_time"),
                    rs.getLong("table_version"),
                    rs.getLong("shard_count"),
                    rs.getLong("row_count"),
                    rs.getLong("compressed_size"),
                    rs.getLong("uncompressed_size"));
        }
    }
}
