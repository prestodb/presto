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
package com.facebook.presto.spi.derivedColumns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class DerivedColumnSpec
{
    private final DerivedColumnType derivedColumnType;
    private final String derivedColumnExpression;
    private final String derivedColumnName;

    @JsonCreator
    public DerivedColumnSpec(
            @JsonProperty("derivedColumnType") DerivedColumnType derivedColumnType,
            @JsonProperty("derivedColumnExpression") String derivedColumnExpression,
            @JsonProperty("derivedColumnName") String derivedColumnName)
    {
        this.derivedColumnType = derivedColumnType;
        this.derivedColumnExpression = derivedColumnExpression;
        this.derivedColumnName = derivedColumnName;
    }

    @JsonProperty
    public DerivedColumnType getDerivedColumnType()
    {
        return derivedColumnType;
    }

    @JsonProperty
    public String getDerivedColumnExpression()
    {
        return derivedColumnExpression;
    }

    @JsonProperty
    public String getDerivedColumnName()
    {
        return derivedColumnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DerivedColumnSpec that = (DerivedColumnSpec) o;
        return derivedColumnType == that.derivedColumnType
                && Objects.equals(derivedColumnExpression, that.derivedColumnExpression)
                && Objects.equals(derivedColumnName, that.derivedColumnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(derivedColumnType, derivedColumnExpression, derivedColumnName);
    }

    @Override
    public String toString()
    {
        return "DerivedColumnSpec{" +
                "derivedColumnType=" + derivedColumnType +
                ", derivedColumnExpression='" + derivedColumnExpression + '\'' +
                ", derivedColumnName='" + derivedColumnName + '\'' +
                '}';
    }
}
