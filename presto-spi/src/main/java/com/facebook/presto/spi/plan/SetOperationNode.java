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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public abstract class SetOperationNode
        extends PlanNode
{
    private final List<PlanNode> sources;
    private final Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    protected SetOperationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("outputToInputs") Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs)
    {
        super(id);

        requireNonNull(sources, "sources is null");
        checkArgument(!sources.isEmpty(), "Must have at least one source");
        requireNonNull(outputToInputs, "outputToInputs is null");

        this.sources = unmodifiableList(new ArrayList<>(sources));
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> copiedMap = new LinkedHashMap<>();
        outputToInputs.forEach((key, value) -> copiedMap.put(key, unmodifiableList(new ArrayList<>(value))));
        this.outputToInputs = unmodifiableMap(copiedMap);
        this.outputVariables = unmodifiableList(new ArrayList<>(outputVariables));

        for (Collection<VariableReferenceExpression> inputs : this.outputToInputs.values()) {
            checkArgument(
                    inputs.size() == this.sources.size(),
                    format("Every source needs to map its variables to an output %s operation variables", this.getClass().getSimpleName()));
        }

        // Make sure each source positionally corresponds to their variable values in the Multimap
        for (int i = 0; i < sources.size(); i++) {
            for (List<VariableReferenceExpression> expectedInputs : this.outputToInputs.values()) {
                checkArgument(sources.get(i).getOutputVariables().contains(expectedInputs.get(i)), "Source does not provide required variables");
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
    public Map<VariableReferenceExpression, List<VariableReferenceExpression>> getVariableMapping()
    {
        return outputToInputs;
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    public List<VariableReferenceExpression> sourceOutputLayout(int sourceIndex)
    {
        // Make sure the sourceOutputLayout variables are listed in the same order as the corresponding output variables
        return unmodifiableList(getOutputVariables().stream().map(variable -> outputToInputs.get(variable).get(sourceIndex)).collect(toList()));
    }

    /**
     * Returns the output to input variable mapping for the given source channel
     */
    public Map<VariableReferenceExpression, VariableReferenceExpression> sourceVariableMap(int sourceIndex)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> result = new LinkedHashMap<>();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : outputToInputs.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get(sourceIndex));
        }

        return unmodifiableMap(result);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
