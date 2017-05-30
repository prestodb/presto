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

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.raptorx.util.DatabaseUtil.utf8String;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TableSummary
{
    private final long tableId;
    private final String tableName;
    private final long schemaId;
    private final long createTime;
    private final long updateTime;
    private final long tableVersion;
    private final long rowCount;

    public TableSummary(
            long tableId,
            String tableName,
            long schemaId,
            long createTime,
            long updateTime,
            long tableVersion,
            long rowCount)
    {
        this.tableId = tableId;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaId = schemaId;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.tableVersion = tableVersion;
        this.rowCount = rowCount;
    }

    public long getTableId()
    {
        return tableId;
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getSchemaId()
    {
        return schemaId;
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

    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("tableName", tableName)
                .add("schemaId", schemaId)
                .add("createTime", createTime)
                .add("updateTime", updateTime)
                .add("tableVersion", tableVersion)
                .add("rowCount", rowCount)
                .toString();
    }

    public static class Mapper
            implements RowMapper<TableSummary>
    {
        @Override
        public TableSummary map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new TableSummary(
                    rs.getLong("table_id"),
                    utf8String(rs.getBytes("table_name")),
                    rs.getLong("schema_id"),
                    rs.getLong("create_time"),
                    rs.getLong("update_time"),
                    rs.getLong("table_version"),
                    rs.getLong("row_count"));
        }
    }
}
