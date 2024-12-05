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
package com.facebook.presto.spi.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 *     Represents a column statistic that should be computed during an {@code ANALYZE} query. The
 *     field {@code sqlExpression} denotes whether the value of the {@code function} field
 *     represents a simple function name, or a complex SQL expression.
 * </p>
 * <p>
 *     In the case of a SQL expression, it should parse to a valid function call that may use
 *     constant-value arguments or references to specific column names in the arguments. Column
 *     references must be properly quoted, as the string should be valid SQL. The SQL itself parses
 *     as a function, so it should always begin with a {@code RETURN} keyword. Any references
 *     columns used in the SQL expression as function arguments should be added to the
 *     {@code columnArguments} field.
 * </p>
 * <p>
 *     Example: Suppose you want to compute the column statistic is of a t-digest with a
 *     configurable weight and compression for a column named "x". The {@code function} should be:
 *     <br>
 *     {@code "RETURN tdigest_agg("x", 1, 200)"}
 *     <p>
 *         Additionally, the {@code columnArguments} field should be populated with {@code ["x"]}
 *     </p>
 * </p>
 */
public class ColumnStatisticMetadata
{
    private final String columnName;
    private final ColumnStatisticType statisticType;
    private final String function;
    private final List<String> columnArguments;
    private final boolean sqlExpression;

    @JsonCreator
    public ColumnStatisticMetadata(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("statisticType") ColumnStatisticType statisticType,
            @JsonProperty("function") String function,
            @JsonProperty("columnArguments") List<String> columnArguments,
            @JsonProperty("sqlExpression") boolean sqlExpression)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.statisticType = requireNonNull(statisticType, "statisticType is null");
        this.function = requireNonNull(function, "functionName is null");
        this.columnArguments = requireNonNull(columnArguments, "additionalArguments is null");
        this.sqlExpression = sqlExpression;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public ColumnStatisticType getStatisticType()
    {
        return statisticType;
    }

    @JsonProperty
    public String getFunction()
    {
        return function;
    }

    @JsonProperty
    public List<String> getColumnArguments()
    {
        return columnArguments;
    }

    @JsonProperty
    public boolean isSqlExpression()
    {
        return sqlExpression;
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
        ColumnStatisticMetadata that = (ColumnStatisticMetadata) o;
        return Objects.equals(columnName, that.columnName) &&
                statisticType == that.statisticType &&
                Objects.equals(function, that.function) &&
                Objects.equals(columnArguments, that.columnArguments) &&
                Objects.equals(sqlExpression, that.sqlExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, statisticType, function, columnArguments, sqlExpression);
    }

    @Override
    public String toString()
    {
        return "ColumnStatisticMetadata{" +
                "columnName='" + columnName + '\'' +
                ", statisticType=" + statisticType +
                ", function=" + function +
                ", columnArguments=" + columnArguments +
                ", sqlExpression=" + sqlExpression +
                '}';
    }
}
