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
import static java.util.Objects.requireNonNull;

public class SchemaInfo
{
    private final long schemaId;
    private final String schemaName;

    public SchemaInfo(long schemaId, String schemaName)
    {
        this.schemaId = schemaId;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public static class Mapper
            implements RowMapper<SchemaInfo>
    {
        @Override
        public SchemaInfo map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new SchemaInfo(
                    rs.getLong("schema_id"),
                    utf8String(rs.getBytes("schema_name")));
        }
    }
}
