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
package com.facebook.presto.spi.plan;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class Ordering
{
    private final VariableReferenceExpression variable;
    private final SortOrder sortOrder;

    @JsonCreator
    public Ordering(
            @JsonProperty("variable") VariableReferenceExpression variable,
            @JsonProperty("sortOrder") SortOrder sortOrder)
    {
        this.variable = requireNonNull(variable, "variable is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
    }

    @JsonProperty
    public VariableReferenceExpression getVariable()
    {
        return variable;
    }

    @JsonProperty
    public SortOrder getSortOrder()
    {
        return sortOrder;
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
        Ordering that = (Ordering) o;
        return Objects.equals(variable, that.variable) &&
                Objects.equals(sortOrder, that.sortOrder);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variable, sortOrder);
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("variable='").append(variable).append('\'');
        stringBuilder.append(", sortOrder='").append(sortOrder).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }
}
