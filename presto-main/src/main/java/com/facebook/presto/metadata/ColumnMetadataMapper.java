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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.tuple.TupleInfo;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnMetadataMapper
        implements ResultSetMapper<ColumnMetadata>
{
    public ColumnMetadata map(int index, ResultSet r, StatementContext ctx)
            throws SQLException
    {
        String name = r.getString("column_name");
        TupleInfo.Type type = TupleInfo.Type.fromName(r.getString("data_type"));
        int ordinalPosition = r.getInt("ordinal_position");
        return new ColumnMetadata(name, type.toColumnType(), ordinalPosition, false);
    }
}
