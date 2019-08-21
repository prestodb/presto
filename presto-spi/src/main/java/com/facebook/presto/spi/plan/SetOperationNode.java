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
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

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
            @JsonProperty("outputToInputs") Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs)
    {
        super(id);

        requireNonNull(sources, "sources is null");
        checkArgument(!sources.isEmpty(), "Must have at least one source");
        requireNonNull(outputToInputs, "outputToInputs is null");

        this.sources = copyAsImmutableList(sources);
        this.outputToInputs = unmodifiableMap(outputToInputs.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> copyAsImmutableList(entry.getValue()))));
        this.outputVariables = copyAsImmutableList(outputToInputs.keySet());

        for (Collection<VariableReferenceExpression> inputs : this.outputToInputs.values()) {
            checkArgument(inputs.size() == this.sources.size(), "Every source needs to map its symbols to an output %s operation symbol", this.getClass().getSimpleName());
        }

        // Make sure each source positionally corresponds to their Symbol values in the Multimap
        for (int i = 0; i < sources.size(); i++) {
            for (List<VariableReferenceExpression> expectedInputs : this.outputToInputs.values()) {
                checkArgument(sources.get(i).getOutputVariables().contains(expectedInputs.get(i)), "Source does not provide required symbols");
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
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    private static <T> List<T> copyAsImmutableList(Collection<T> list)
    {
        return unmodifiableList(new ArrayList<>(list));
    }

    private static void checkArgument(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new IllegalArgumentException(String.format(message, arguments));
        }
    }
}
