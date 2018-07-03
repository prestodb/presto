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
package com.facebook.presto.jdbc;

import com.google.common.collect.ImmutableList;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.jdbc.util.MetaDataUtils.getClassNameByType;
import static java.util.Objects.requireNonNull;

public class PrestoParameterMetaData
        implements ParameterMetaData
{
    private final List<ParameterInfo> parameterInfo;

    public PrestoParameterMetaData(List<ParameterInfo> parameterInfo)
    {
        this.parameterInfo = ImmutableList.copyOf(requireNonNull(parameterInfo, "parameterInfo is null"));
    }

    @Override
    public int getParameterCount() throws SQLException
    {
        return parameterInfo.size();
    }

    @Override
    public int isNullable(int param)
            throws SQLException
    {
        TypeInfo.Nullable nullable = parameter(param).getNullable();
        switch (nullable) {
            case NO_NULLS:
                return parameterNoNulls;
            case NULLABLE:
                return parameterNullable;
            case UNKNOWN:
                return parameterNullableUnknown;
        }
        throw new SQLException("Unhandled nullable type: " + nullable);
    }

    @Override
    public boolean isSigned(int param) throws SQLException
    {
        return parameter(param).isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException
    {
        return parameter(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException
    {
        return parameter(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException
    {
        return parameter(param).getParameterType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException
    {
        return parameter(param).getParameterTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException
    {
        return getClassNameByType(parameter(param).getParameterType());
    }

    @Override
    public int getParameterMode(int param) throws SQLException
    {
        return parameterModeIn;
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

    private ParameterInfo parameter(int param)
            throws SQLException
    {
        if ((param <= 0) || (param > parameterInfo.size())) {
            throw new SQLException("Invalid parameter index: " + param);
        }
        return parameterInfo.get(param - 1);
    }
}
