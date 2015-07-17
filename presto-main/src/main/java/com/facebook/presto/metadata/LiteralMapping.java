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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public final class LiteralMapping
{
    private final String literalName;
    private final int functionParameterIndex;

    @JsonCreator
    public LiteralMapping(
            @JsonProperty("literalName") String literalName,
            @JsonProperty("functionParameterIndex") int functionParameterIndex)
    {
        this.literalName = checkNotNull(literalName);
        this.functionParameterIndex = functionParameterIndex;
    }

    @JsonProperty
    public String getLiteralName()
    {
        return literalName;
    }

    @JsonProperty
    public int getFunctionParameterIndex()
    {
        return functionParameterIndex;
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

        LiteralMapping other = (LiteralMapping) o;

        return Objects.equals(this.literalName, other.literalName) &&
                Objects.equals(this.functionParameterIndex, other.functionParameterIndex);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(literalName, functionParameterIndex);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("literalName", literalName)
                .add("functionParameterIndex", functionParameterIndex)
                .toString();
    }
}
