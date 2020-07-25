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
package com.facebook.presto.benchmark.source;

import com.facebook.airlift.json.JsonCodec;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;

public class StringToStringMapColumnMapper
        implements ColumnMapper<Map<String, String>>
{
    public static final JsonCodec<Map<String, String>> MAP_CODEC = mapJsonCodec(String.class, String.class);

    @Override
    public Map<String, String> map(ResultSet resultSet, int columnNumber, StatementContext ctx)
            throws SQLException
    {
        String columnValue = resultSet.getString(columnNumber);
        return columnValue == null ? null : MAP_CODEC.fromJson(columnValue);
    }
}
