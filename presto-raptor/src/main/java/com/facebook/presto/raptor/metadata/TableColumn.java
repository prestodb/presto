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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableColumn
{
    private final SchemaTableName table;
    private final String columnName;
    private final int ordinalPosition;
    private final Type dataType;
    private final long columnId;

    public TableColumn(SchemaTableName table, String columnName, int ordinalPosition, Type dataType, long columnId)
    {
        this.table = checkNotNull(table, "table is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        checkArgument(ordinalPosition >= 0, "ordinal position is negative");
        this.ordinalPosition = ordinalPosition;
        this.dataType = checkNotNull(dataType, "dataType is null");
        this.columnId = columnId;
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public Type getDataType()
    {
        return dataType;
    }

    public long getColumnId()
    {
        return columnId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, columnName, ordinalPosition, dataType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableColumn o = (TableColumn) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(columnName, o.columnName) &&
                Objects.equals(ordinalPosition, o.ordinalPosition) &&
                Objects.equals(dataType, o.dataType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("columnName", columnName)
                .add("ordinalPosition", ordinalPosition)
                .add("dataType", dataType)
                .toString();
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, dataType, ordinalPosition, false);
    }

    public ColumnInfo toColumnInfo()
    {
        return new ColumnInfo(columnId, dataType);
    }

    public static class Mapper
            implements ResultSetMapper<TableColumn>
    {
        private final TypeManager typeManager;

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        public TableColumn map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            SchemaTableName table = new SchemaTableName(
                    r.getString("schema_name"),
                    r.getString("table_name"));

            String typeName = r.getString("data_type");
            Type type = typeManager.getType(parseTypeSignature(typeName));
            checkArgument(type != null, "Unknown type %s", typeName);

            return new TableColumn(
                    table,
                    r.getString("column_name"),
                    r.getInt("ordinal_position"),
                    type,
                    r.getLong("column_id"));
        }
    }
}
