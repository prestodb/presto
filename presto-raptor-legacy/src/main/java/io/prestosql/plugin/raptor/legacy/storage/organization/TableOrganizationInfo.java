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
package com.facebook.presto.raptor.storage.organization;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalLong;

import static com.facebook.presto.raptor.util.DatabaseUtil.getOptionalLong;
import static java.util.Objects.requireNonNull;

public class TableOrganizationInfo
{
    private final long tableId;
    private final OptionalLong lastStartTimeMillis;

    public TableOrganizationInfo(long tableId, OptionalLong lastStartTimeMillis)
    {
        this.tableId = tableId;
        this.lastStartTimeMillis = requireNonNull(lastStartTimeMillis, "lastStartTimeMillis is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public OptionalLong getLastStartTimeMillis()
    {
        return lastStartTimeMillis;
    }

    public static class Mapper
            implements ResultSetMapper<TableOrganizationInfo>
    {
        @Override
        public TableOrganizationInfo map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new TableOrganizationInfo(
                    r.getLong("table_id"),
                    getOptionalLong(r, "last_start_time"));
        }
    }
}
