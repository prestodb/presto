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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class DataOrganizationSpecification
{
    private final List<VariableReferenceExpression> partitionBy;
    private final Optional<OrderingScheme> orderingScheme;

    @JsonCreator
    public DataOrganizationSpecification(
            @JsonProperty("partitionBy") List<VariableReferenceExpression> partitionBy,
            @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme)
    {
        requireNonNull(partitionBy, "partitionBy is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.partitionBy = unmodifiableList(new ArrayList<>(partitionBy));
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
    }

    @JsonProperty
    public List<VariableReferenceExpression> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionBy, orderingScheme);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DataOrganizationSpecification other = (DataOrganizationSpecification) obj;

        return Objects.equals(this.partitionBy, other.partitionBy) &&
                Objects.equals(this.orderingScheme, other.orderingScheme);
    }
}
