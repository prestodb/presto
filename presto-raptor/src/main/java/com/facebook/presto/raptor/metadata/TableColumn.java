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
import java.util.OptionalInt;

import static com.facebook.presto.raptor.util.DatabaseUtil.getOptionalInt;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableColumn
{
    private final SchemaTableName table;
    private final String columnName;
    private final Type dataType;
    private final long columnId;
    private final OptionalInt bucketOrdinal;
    private final OptionalInt sortOrdinal;
    private final boolean temporal;

    public TableColumn(SchemaTableName table, String columnName, Type dataType, long columnId, OptionalInt bucketOrdinal, OptionalInt sortOrdinal, boolean temporal)
    {
        this.table = requireNonNull(table, "table is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.columnId = columnId;
        this.bucketOrdinal = requireNonNull(bucketOrdinal, "bucketOrdinal is null");
        this.sortOrdinal = requireNonNull(sortOrdinal, "sortOrdinal is null");
        this.temporal = temporal;
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getDataType()
    {
        return dataType;
    }

    public long getColumnId()
    {
        return columnId;
    }

    public OptionalInt getBucketOrdinal()
    {
        return bucketOrdinal;
    }

    public OptionalInt getSortOrdinal()
    {
        return sortOrdinal;
    }

    public boolean isTemporal()
    {
        return temporal;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("columnId", columnId)
                .add("columnName", columnName)
                .add("dataType", dataType)
                .toString();
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, dataType);
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
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
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
                    type,
                    r.getLong("column_id"),
                    getOptionalInt(r, "bucket_ordinal_position"),
                    getOptionalInt(r, "sort_ordinal_position"),
                    r.getBoolean("temporal"));
        }
    }
}
