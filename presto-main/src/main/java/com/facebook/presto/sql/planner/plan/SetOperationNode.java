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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SetOperationNode
        extends InternalPlanNode
{
    private final List<PlanNode> sources;
    private final ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputToInputs;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    protected SetOperationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("outputToInputs") ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputToInputs)
    {
        super(id);

        requireNonNull(sources, "sources is null");
        checkArgument(!sources.isEmpty(), "Must have at least one source");
        requireNonNull(outputToInputs, "outputToInputs is null");

        this.sources = ImmutableList.copyOf(sources);
        this.outputToInputs = ImmutableListMultimap.copyOf(outputToInputs);
        this.outputVariables = ImmutableList.copyOf(outputToInputs.keySet());

        for (Collection<VariableReferenceExpression> inputs : this.outputToInputs.asMap().values()) {
            checkArgument(inputs.size() == this.sources.size(), "Every source needs to map its symbols to an output %s operation symbol", this.getClass().getSimpleName());
        }

        // Make sure each source positionally corresponds to their Symbol values in the Multimap
        for (int i = 0; i < sources.size(); i++) {
            for (Collection<VariableReferenceExpression> expectedInputs : this.outputToInputs.asMap().values()) {
                checkArgument(sources.get(i).getOutputVariables().contains(Iterables.get(expectedInputs, i)), "Source does not provide required symbols");
            }
        }
    }

    @Override
    @JsonProperty
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @JsonProperty
    public ListMultimap<VariableReferenceExpression, VariableReferenceExpression> getVariableMapping()
    {
        return outputToInputs;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    public List<VariableReferenceExpression> sourceOutputLayout(int sourceIndex)
    {
        // Make sure the sourceOutputSymbolLayout symbols are listed in the same order as the corresponding output symbols
        return getOutputVariables().stream()
                .map(variable -> outputToInputs.get(variable).get(sourceIndex))
                .collect(toImmutableList());
    }

    /**
     * Returns the output to input variable mapping for the given source channel
     */
    public Map<VariableReferenceExpression, VariableReferenceExpression> sourceVariableMap(int sourceIndex)
    {
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> builder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Collection<VariableReferenceExpression>> entry : outputToInputs.asMap().entrySet()) {
            builder.put(entry.getKey(), Iterables.get(entry.getValue(), sourceIndex));
        }

        return builder.build();
    }
    /**
     * Returns the input to output symbol mapping for the given source channel.
     * A single input symbol can map to multiple output symbols, thus requiring a Multimap.
     */
    public Multimap<VariableReferenceExpression, VariableReferenceExpression> outputMap(int sourceIndex)
    {
        return FluentIterable.from(getOutputVariables())
                .toMap(output -> outputToInputs.get(output).get(sourceIndex))
                .asMultimap()
                .inverse();
    }
}
