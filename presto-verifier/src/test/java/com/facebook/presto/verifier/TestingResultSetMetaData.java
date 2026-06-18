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

import com.facebook.presto.common.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class TestingResultSetMetaData
        implements ResultSetMetaData
{
    public static class ColumnInfo
    {
        private final String columnName;
        private final TypeSignature columnType;

        public ColumnInfo(String columnName, String columnType)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.columnType = parseTypeSignature(columnType);
        }

        public String getColumnName()
        {
            return columnName;
        }

        public String getColumnTypeName()
        {
            return columnType.toString();
        }
    }

    private final List<ColumnInfo> columns;

    public TestingResultSetMetaData(List<ColumnInfo> columns)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columnInfo is null"));
    }

    @Override
    public int getColumnCount()
    {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCaseSensitive(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSearchable(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCurrency(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int isNullable(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSigned(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnDisplaySize(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnLabel(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnName(int column)
            throws SQLException
    {
        return column(column).getColumnName();
    }

    @Override
    public int getPrecision(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getScale(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchemaName(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalogName(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnType(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnTypeName(int column)
            throws SQLException
    {
        return column(column).getColumnTypeName();
    }

    @Override
    public boolean isReadOnly(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWritable(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDefinitelyWritable(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnClassName(int column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
    {
        throw new UnsupportedOperationException();
    }

    private ColumnInfo column(int column)
            throws SQLException
    {
        if ((column <= 0) || (column > columns.size())) {
            throw new SQLException("Invalid column index: " + column);
        }
        return columns.get(column - 1);
    }
}
