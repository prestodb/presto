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
package com.facebook.presto.verifier;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryPairMapper
        implements ResultSetMapper<QueryPair>
{
    @Override
    public QueryPair map(int index, ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        return new QueryPair(
                rs.getString("suite"),
                rs.getString("name"),
                new Query(rs.getString("test_catalog"), rs.getString("test_schema"), rs.getString("test_query")),
                new Query(rs.getString("control_catalog"), rs.getString("control_schema"), rs.getString("control_query")));
    }
}
