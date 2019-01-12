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

import com.google.common.primitives.Ints;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrestoArray
        implements Array
{
    private final String elementTypeName;
    private final int elementType;
    private final Object[] array;

    PrestoArray(String elementTypeName, int elementType, List<?> array)
    {
        this.elementTypeName = requireNonNull(elementTypeName, "elementType is null");
        this.elementType = elementType;
        this.array = requireNonNull(array, "array is null").toArray();
    }

    @Override
    public String getBaseTypeName()
    {
        return elementTypeName;
    }

    @Override
    public int getBaseType()
    {
        return elementType;
    }

    @Override
    public Object getArray()
    {
        return array.clone();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public Object getArray(long index, int count)
            throws SQLException
    {
        int arrayOffset = Ints.saturatedCast(index - 1);
        if (index < 1 || count < 0 || (arrayOffset + count) > array.length) {
            throw new SQLException("Index out of bounds");
        }
        return Arrays.copyOfRange(array, arrayOffset, arrayOffset + count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getResultSet not supported");
    }

    @Override
    public void free()
    {
        // no-op
    }
}
