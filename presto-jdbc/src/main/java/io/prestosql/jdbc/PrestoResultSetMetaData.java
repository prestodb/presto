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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PrestoResultSetMetaData
        implements ResultSetMetaData
{
    private final List<ColumnInfo> columnInfo;

    PrestoResultSetMetaData(List<ColumnInfo> columnInfo)
    {
        this.columnInfo = ImmutableList.copyOf(requireNonNull(columnInfo, "columnInfo is null"));
    }

    @Override
    public int getColumnCount()
            throws SQLException
    {
        return columnInfo.size();
    }

    @Override
    public boolean isAutoIncrement(int column)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isSearchable(int column)
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean isCurrency(int column)
            throws SQLException
    {
        return column(column).isCurrency();
    }

    @Override
    public int isNullable(int column)
            throws SQLException
    {
        ColumnInfo.Nullable nullable = column(column).getNullable();
        switch (nullable) {
            case NO_NULLS:
                return columnNoNulls;
            case NULLABLE:
                return columnNullable;
            case UNKNOWN:
                return columnNullableUnknown;
        }
        throw new SQLException("Unhandled nullable type: " + nullable);
    }

    @Override
    public boolean isSigned(int column)
            throws SQLException
    {
        return column(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column)
            throws SQLException
    {
        return column(column).getColumnDisplaySize();
    }

    @Override
    public String getColumnLabel(int column)
            throws SQLException
    {
        return column(column).getColumnLabel();
    }

    @Override
    public String getColumnName(int column)
            throws SQLException
    {
        return column(column).getColumnName();
    }

    @Override
    public int getPrecision(int column)
            throws SQLException
    {
        return column(column).getPrecision();
    }

    @Override
    public int getScale(int column)
            throws SQLException
    {
        return column(column).getScale();
    }

    @Override
    public String getTableName(int column)
            throws SQLException
    {
        return column(column).getTableName();
    }

    @Override
    public String getSchemaName(int column)
            throws SQLException
    {
        return column(column).getSchemaName();
    }

    @Override
    public String getCatalogName(int column)
            throws SQLException
    {
        return column(column).getCatalogName();
    }

    @Override
    public int getColumnType(int column)
            throws SQLException
    {
        return column(column).getColumnType();
    }

    @Override
    public String getColumnTypeName(int column)
            throws SQLException
    {
        return column(column).getColumnTypeName();
    }

    @Override
    public boolean isReadOnly(int column)
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean isWritable(int column)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column)
            throws SQLException
    {
        return false;
    }

    @Override
    public String getColumnClassName(int column)
            throws SQLException
    {
        // see javax.sql.rowset.RowSetMetaDataImpl
        switch (column(column).getColumnType()) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class.getName();
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class.getName();
            case Types.TINYINT:
                return Byte.class.getName();
            case Types.SMALLINT:
                return Short.class.getName();
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.REAL:
                return Float.class.getName();
            case Types.FLOAT:
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return "byte[]";
            case Types.DATE:
                return Date.class.getName();
            case Types.TIME:
                return Time.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.BLOB:
                return Blob.class.getName();
            case Types.CLOB:
                return Clob.class.getName();
            case Types.ARRAY:
                return Array.class.getName();
        }
        return String.class.getName();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private ColumnInfo column(int column)
            throws SQLException
    {
        if ((column <= 0) || (column > columnInfo.size())) {
            throw new SQLException("Invalid column index: " + column);
        }
        return columnInfo.get(column - 1);
    }
}
