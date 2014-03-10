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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ColumnMetadataMapper
        implements ResultSetMapper<ColumnMetadata>
{
    private final TypeManager typeManager;

    @Inject
    public ColumnMetadataMapper(TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    public ColumnMetadata map(int index, ResultSet r, StatementContext ctx)
            throws SQLException
    {
        String name = r.getString("column_name");
        String typeName = r.getString("data_type");
        Type type = typeManager.getType(typeName);
        checkArgument(type != null, "Unknown type %s", typeName);
        int ordinalPosition = r.getInt("ordinal_position");
        return new ColumnMetadata(name, type, ordinalPosition, false);
    }
}
