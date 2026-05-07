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

package com.facebook.presto.iceberg.derivedColumn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class DerivedColumnArgumentSpec
{
    private final Integer argumentIndex;
    private final String argumentType;
    private final String argumentValue;
    private final DerivedColumnRef columnRef;

    @JsonCreator
    public DerivedColumnArgumentSpec(
            @JsonProperty("argumentIndex") Integer argumentIndex,
            @JsonProperty("argumentType") String argumentType,
            @JsonProperty("argumentValue") String argumentValue,
            @JsonProperty("columnRef") DerivedColumnRef columnRef)
    {
        this.argumentIndex = argumentIndex;
        this.argumentType = argumentType;
        this.argumentValue = argumentValue;
        this.columnRef = columnRef;
    }

    @JsonProperty
    public DerivedColumnRef getColumnRef()
    {
        return columnRef;
    }

    @JsonProperty
    public Integer getArgumentIndex()
    {
        return argumentIndex;
    }

    @JsonProperty
    public String getArgumentType()
    {
        return argumentType;
    }

    @JsonProperty
    public String getArgumentValue()
    {
        return argumentValue;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DerivedColumnArgumentSpec that = (DerivedColumnArgumentSpec) o;
        return Objects.equal(argumentIndex, that.argumentIndex)
                && Objects.equal(argumentType, that.argumentType)
                && Objects.equal(argumentValue, that.argumentValue)
                && columnRef == that.columnRef;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(argumentIndex, argumentType, argumentValue, columnRef);
    }
}
