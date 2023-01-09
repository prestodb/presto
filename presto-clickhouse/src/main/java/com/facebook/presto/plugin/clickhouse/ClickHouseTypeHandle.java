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
package com.facebook.presto.plugin.clickhouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ClickHouseTypeHandle
{
    private final int jdbcType;
    private final Optional<String> jdbcTypeName;
    private final int columnSize;
    private final int decimalDigits;
    private final Optional<Integer> arrayDimensions;
    private final Optional<CaseSensitivity> caseSensitivity;

    @JsonCreator
    public ClickHouseTypeHandle(
            @JsonProperty("jdbcType") int jdbcType,
            @JsonProperty("jdbcTypeName") Optional<String> jdbcTypeName,
            @JsonProperty("columnSize") int columnSize,
            @JsonProperty("decimalDigits") int decimalDigits,
            @JsonProperty("arrayDimensions") Optional<Integer> arrayDimensions,
            @JsonProperty("caseSensitivity") Optional<CaseSensitivity> caseSensitivity)
    {
        this.jdbcType = jdbcType;
        this.jdbcTypeName = requireNonNull(jdbcTypeName, "jdbcTypeName is null");
        this.columnSize = requireNonNull(columnSize, "columnSize is null");
        this.decimalDigits = requireNonNull(decimalDigits, "decimalDigits is null");
        this.arrayDimensions = requireNonNull(arrayDimensions, "arrayDimensions is null");
        this.caseSensitivity = requireNonNull(caseSensitivity, "caseSensitivity is null");
    }

    @JsonProperty
    public int getJdbcType()
    {
        return jdbcType;
    }

    @JsonProperty
    public Optional<String> getJdbcTypeName()
    {
        return jdbcTypeName;
    }

    @JsonProperty
    public int getColumnSize()
    {
        return columnSize;
    }

    @JsonIgnore
    public int getRequiredColumnSize()
    {
        return getColumnSize();
    }

    @JsonProperty
    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    @JsonIgnore
    public int getRequiredDecimalDigits()
    {
        return getDecimalDigits();
    }

    @JsonProperty
    public Optional<Integer> getArrayDimensions()
    {
        return arrayDimensions;
    }

    @JsonProperty
    public Optional<CaseSensitivity> getCaseSensitivity()
    {
        return caseSensitivity;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jdbcType, jdbcTypeName, columnSize, decimalDigits, arrayDimensions);
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
        ClickHouseTypeHandle that = (ClickHouseTypeHandle) o;
        return jdbcType == that.jdbcType &&
                Objects.equals(columnSize, that.columnSize) &&
                Objects.equals(decimalDigits, that.decimalDigits) &&
                Objects.equals(jdbcTypeName, that.jdbcTypeName) &&
                Objects.equals(arrayDimensions, that.arrayDimensions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("jdbcType", jdbcType)
                .add("jdbcTypeName", jdbcTypeName.orElse(null))
                .add("columnSize", columnSize)
                .add("decimalDigits", decimalDigits)
                .add("arrayDimensions", arrayDimensions.orElse(null))
                .toString();
    }
}
