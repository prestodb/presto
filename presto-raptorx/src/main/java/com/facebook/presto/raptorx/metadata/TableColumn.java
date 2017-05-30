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

import static com.google.common.base.MoreObjects.toStringHelper;

public class TableColumn
{
    private final long tableId;
    private final long columnId;

    public TableColumn(long tableId, long columnId)
    {
        this.tableId = tableId;
        this.columnId = columnId;
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getColumnId()
    {
        return columnId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("columnId", columnId)
                .toString();
    }

    public static class Mapper
            implements RowMapper<TableColumn>
    {
        @Override
        public TableColumn map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new TableColumn(
                    rs.getLong("table_id"),
                    rs.getLong("column_id"));
        }
    }
}
