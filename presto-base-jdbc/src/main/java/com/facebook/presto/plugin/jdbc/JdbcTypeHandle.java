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
package com.facebook.presto.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class JdbcTypeHandle
{
    private final int jdbcType;
    private final String jdbcTypeName;
    private final int columnSize;
    private final int decimalDigits;

    @JsonCreator
    public JdbcTypeHandle(
            @JsonProperty("jdbcType") int jdbcType,
            @JsonProperty("jdbcTypeName") String jdbcTypeName,
            @JsonProperty("columnSize") int columnSize,
            @JsonProperty("decimalDigits") int decimalDigits)
    {
        this.jdbcType = jdbcType;
        this.jdbcTypeName = requireNonNull(jdbcTypeName, "jdbcTypeName is null");
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
    }

    @JsonProperty
    public int getJdbcType()
    {
        return jdbcType;
    }

    @JsonProperty
    public String getJdbcTypeName()
    {
        return jdbcTypeName;
    }

    @JsonProperty
    public int getColumnSize()
    {
        return columnSize;
    }

    @JsonProperty
    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jdbcType, jdbcTypeName, columnSize, decimalDigits);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcTypeHandle that = (JdbcTypeHandle) o;
        return jdbcType == that.jdbcType &&
                columnSize == that.columnSize &&
                decimalDigits == that.decimalDigits &&
                Objects.equals(jdbcTypeName, that.jdbcTypeName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("jdbcType", jdbcType)
                .add("jdbcTypeName", jdbcTypeName)
                .add("columnSize", columnSize)
                .add("decimalDigits", decimalDigits)
                .toString();
    }
}
