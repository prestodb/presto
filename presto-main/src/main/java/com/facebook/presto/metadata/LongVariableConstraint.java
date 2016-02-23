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

package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

public class LongVariableConstraint
{
    private final String name;
    private final String calculation;

    @JsonCreator
    public LongVariableConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("calculation") @Nullable String calculation)
    {
        this.name = name;
        this.calculation = calculation;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getCalculation()
    {
        return calculation;
    }

    @Override
    public String toString()
    {
        return name + ":" + calculation;
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
        LongVariableConstraint that = (LongVariableConstraint) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(calculation, that.calculation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, calculation);
    }
}
