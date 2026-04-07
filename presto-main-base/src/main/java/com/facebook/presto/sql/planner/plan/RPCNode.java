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

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class RPCNode
        extends InternalPlanNode
{
    public enum StreamingMode
    {
        PER_ROW,
        BATCH
    }

    private final PlanNode source;
    private final String functionName;
    private final List<RowExpression> arguments;
    private final List<String> argumentColumns;
    private final VariableReferenceExpression outputVariable;
    private final List<VariableReferenceExpression> outputVariables;
    private final StreamingMode streamingMode;
    private final int dispatchBatchSize;

    @JsonCreator
    public RPCNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("functionName") String functionName,
            @JsonProperty("arguments") List<RowExpression> arguments,
            @JsonProperty("argumentColumns") List<String> argumentColumns,
            @JsonProperty("outputVariable") VariableReferenceExpression outputVariable,
            @JsonProperty("streamingMode") StreamingMode streamingMode,
            @JsonProperty("dispatchBatchSize") Integer dispatchBatchSize)
    {
        this(sourceLocation, id, Optional.empty(), source, functionName,
                arguments, argumentColumns, outputVariable, streamingMode,
                dispatchBatchSize != null ? dispatchBatchSize : 0);
    }

    public RPCNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            String functionName,
            List<RowExpression> arguments,
            List<String> argumentColumns,
            VariableReferenceExpression outputVariable,
            StreamingMode streamingMode,
            int dispatchBatchSize)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.source = requireNonNull(source, "source is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        this.argumentColumns = ImmutableList.copyOf(requireNonNull(argumentColumns, "argumentColumns is null"));
        checkArgument(
                this.arguments.size() == this.argumentColumns.size(),
                "arguments and argumentColumns must have the same size: arguments=%s, argumentColumns=%s",
                this.arguments.size(),
                this.argumentColumns.size());
        this.outputVariable = requireNonNull(outputVariable, "outputVariable is null");
        this.streamingMode = streamingMode != null ? streamingMode : StreamingMode.PER_ROW;
        this.dispatchBatchSize = dispatchBatchSize;

        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        outputs.addAll(source.getOutputVariables());
        outputs.add(outputVariable);
        this.outputVariables = outputs.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public List<String> getArgumentColumns()
    {
        return argumentColumns;
    }

    @JsonProperty
    public VariableReferenceExpression getOutputVariable()
    {
        return outputVariable;
    }

    @JsonProperty
    public StreamingMode getStreamingMode()
    {
        return streamingMode;
    }

    @JsonProperty
    public int getDispatchBatchSize()
    {
        return dispatchBatchSize;
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitRPC(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new RPCNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                Iterables.getOnlyElement(newChildren),
                functionName,
                arguments,
                argumentColumns,
                outputVariable,
                streamingMode,
                dispatchBatchSize);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new RPCNode(
                getSourceLocation(),
                getId(),
                statsEquivalentPlanNode,
                source,
                functionName,
                arguments,
                argumentColumns,
                outputVariable,
                streamingMode,
                dispatchBatchSize);
    }
}
