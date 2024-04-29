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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ColumnStatisticMetadata
{
    private final String columnName;
    private final ColumnStatisticType statisticType;

    private final String functionName;

    @JsonCreator
    public ColumnStatisticMetadata(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("statisticType") ColumnStatisticType statisticType,
            @JsonProperty("functionName") String functionName)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.statisticType = requireNonNull(statisticType, "statisticType is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
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
    public String getFunctionName()
    {
        return functionName;
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
                Objects.equals(functionName, that.functionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, statisticType, functionName);
    }

    @Override
    public String toString()
    {
        return "ColumnStatisticMetadata{" +
                "columnName='" + columnName + '\'' +
                ", statisticType=" + statisticType +
                ", functionName=" + functionName +
                '}';
    }
}
