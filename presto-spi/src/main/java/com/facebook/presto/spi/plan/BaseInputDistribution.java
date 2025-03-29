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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BaseInputDistribution
        implements InputDistribution
{
    private final List<VariableReferenceExpression> partitionedBy;
    private final Optional<OrderingScheme> orderingScheme;
    private final List<VariableReferenceExpression> inputVariables;

    @JsonCreator
    public BaseInputDistribution(
            @JsonProperty("partitionBy") List<VariableReferenceExpression> partitionedBy,
            @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme,
            @JsonProperty("inputVariables") List<VariableReferenceExpression> inputVariables)
    {
        this.partitionedBy = requireNonNull(partitionedBy, "partitionedBy is null");
        this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        this.inputVariables = requireNonNull(inputVariables, "inputVariables is null");
    }

    @JsonProperty
    @Override
    public List<VariableReferenceExpression> getPartitionBy()
    {
        return partitionedBy;
    }

    @JsonProperty
    @Override
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty
    @Override
    public List<VariableReferenceExpression> getInputVariables()
    {
        return inputVariables;
    }
}
