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

import com.facebook.presto.spi.SchemaTableName;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class ViewResult
{
    private final SchemaTableName name;
    private final String data;

    public ViewResult(SchemaTableName name, String data)
    {
        this.name = requireNonNull(name, "name is null");
        this.data = requireNonNull(data, "data is null");
    }

    public SchemaTableName getName()
    {
        return name;
    }

    public String getData()
    {
        return data;
    }

    public static class Mapper
            implements ResultSetMapper<ViewResult>
    {
        @Override
        public ViewResult map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            SchemaTableName name = new SchemaTableName(
                    r.getString("schema_name"),
                    r.getString("table_name"));
            return new ViewResult(name, r.getString("data"));
        }
    }
}
