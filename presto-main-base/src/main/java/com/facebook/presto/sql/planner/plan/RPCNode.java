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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Objects.requireNonNull;

@Immutable
public class RPCNode
        extends InternalPlanNode
{
    // AUTOMATIC and BATCH_DEFERRED are coordinator-only planning values. The optimizer
    // resolves AUTOMATIC through the execution policy and lowers BATCH_DEFERRED into
    // separate phases, each with physical BATCH mode. A serialized RPCNode carries only
    // PER_ROW or BATCH. The comment is kept outside the enum body because the native protocol
    // generator (java-to-struct-json.py) mis-parses interior enum comments.
    public enum StreamingMode
    {
        PER_ROW,
        BATCH,
        BATCH_DEFERRED,
        AUTOMATIC
    }

    public enum ExecutionPhase
    {
        INLINE,
        SUBMIT,
        POLL,
        COLLECT
    }

    private final ExecutionPhase executionPhase;
    private final PlanNode source;
    private final String functionName;
    private final List<RowExpression> arguments;
    private final List<String> argumentColumns;
    private final VariableReferenceExpression outputVariable;
    private final List<VariableReferenceExpression> outputVariables;
    private final StreamingMode streamingMode;
    private final int dispatchBatchSize;
    private final int recommendedTaskCount;
    private final List<RowExpression> inputGroupingExpressions;

    public RPCNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            PlanNode source,
            String functionName,
            List<RowExpression> arguments,
            List<String> argumentColumns,
            VariableReferenceExpression outputVariable,
            StreamingMode streamingMode,
            Integer dispatchBatchSize)
    {
        this(sourceLocation, id, source, functionName, arguments, argumentColumns,
                outputVariable, streamingMode, dispatchBatchSize, ExecutionPhase.INLINE);
    }

    public RPCNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("functionName") String functionName,
            @JsonProperty("arguments") List<RowExpression> arguments,
            @JsonProperty("argumentColumns") List<String> argumentColumns,
            @JsonProperty("outputVariable") VariableReferenceExpression outputVariable,
            @JsonProperty("streamingMode") StreamingMode streamingMode,
            @JsonProperty("dispatchBatchSize") Integer dispatchBatchSize,
            @JsonProperty("executionPhase") ExecutionPhase executionPhase)
    {
        this(sourceLocation, id, source, functionName, arguments, argumentColumns, outputVariable,
                streamingMode, dispatchBatchSize, executionPhase, ImmutableList.of());
    }

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
            @JsonProperty("dispatchBatchSize") Integer dispatchBatchSize,
            @JsonProperty("executionPhase") ExecutionPhase executionPhase,
            @JsonProperty("inputGroupingExpressions") List<RowExpression> inputGroupingExpressions)
    {
        this(sourceLocation, id, Optional.empty(), source, functionName,
                arguments, argumentColumns, outputVariable, streamingMode,
                dispatchBatchSize != null ? dispatchBatchSize : 0, 0, executionPhase, inputGroupingExpressions);
    }

    public RPCNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            PlanNode source,
            String functionName,
            List<RowExpression> arguments,
            List<String> argumentColumns,
            VariableReferenceExpression outputVariable,
            StreamingMode streamingMode,
            int dispatchBatchSize,
            int recommendedTaskCount)
    {
        this(
                sourceLocation,
                id,
                Optional.empty(),
                source,
                functionName,
                arguments,
                argumentColumns,
                outputVariable,
                streamingMode,
                dispatchBatchSize,
                recommendedTaskCount);
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
            int dispatchBatchSize,
            int recommendedTaskCount)
    {
        this(sourceLocation, id, statsEquivalentPlanNode, source, functionName,
                arguments, argumentColumns, outputVariable, streamingMode,
                dispatchBatchSize, recommendedTaskCount, ExecutionPhase.INLINE);
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
            int dispatchBatchSize,
            int recommendedTaskCount,
            ExecutionPhase executionPhase)
    {
        this(sourceLocation, id, statsEquivalentPlanNode, source, functionName, arguments, argumentColumns,
                outputVariable, streamingMode, dispatchBatchSize, recommendedTaskCount, executionPhase, ImmutableList.of());
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
            int dispatchBatchSize,
            int recommendedTaskCount,
            ExecutionPhase executionPhase,
            List<RowExpression> inputGroupingExpressions)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.executionPhase = executionPhase == null ? ExecutionPhase.INLINE : executionPhase;
        checkArgument(this.executionPhase == ExecutionPhase.INLINE || streamingMode == StreamingMode.BATCH,
                "Deferred RPC phases require BATCH mode");
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
        checkArgument(
                this.streamingMode == StreamingMode.PER_ROW || this.streamingMode == StreamingMode.BATCH,
                "RPCNode streamingMode must be PER_ROW or BATCH, not AUTOMATIC or BATCH_DEFERRED (coordinator-only values); resolve it before constructing the node");
        this.dispatchBatchSize = dispatchBatchSize;
        checkArgument(recommendedTaskCount >= 0, "recommendedTaskCount must be non-negative: %s", recommendedTaskCount);
        this.recommendedTaskCount = recommendedTaskCount;
        this.inputGroupingExpressions = ImmutableList.copyOf(inputGroupingExpressions == null ? ImmutableList.of() : inputGroupingExpressions);

        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        outputs.addAll(source.getOutputVariables());
        outputs.add(outputVariable);
        this.outputVariables = outputs.build();
    }

    @JsonProperty
    public ExecutionPhase getExecutionPhase()
    {
        return executionPhase;
    }

    @JsonIgnore
    public String getDisplayName()
    {
        switch (executionPhase) {
            case SUBMIT:
                return "RpcSubmit";
            case POLL:
                return "RpcPoll";
            case COLLECT:
                return "RpcCollect";
            default:
                return "RPC";
        }
    }

    public RPCNode rewrite(PlanNode source, List<RowExpression> arguments, List<String> argumentColumns, VariableReferenceExpression outputVariable)
    {
        return rewrite(source, arguments, argumentColumns, outputVariable, inputGroupingExpressions);
    }

    public RPCNode rewrite(PlanNode source, List<RowExpression> arguments, List<String> argumentColumns,
            VariableReferenceExpression outputVariable, List<RowExpression> inputGroupingExpressions)
    {
        return new RPCNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), source,
                functionName, arguments, argumentColumns, outputVariable,
                streamingMode, dispatchBatchSize, recommendedTaskCount, executionPhase, inputGroupingExpressions);
    }

    @JsonProperty
    public List<RowExpression> getInputGroupingExpressions()
    {
        return inputGroupingExpressions;
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

    /**
     * Returns the coordinator-only remote task recommendation, or zero for default planning.
     */
    @JsonIgnore
    public int getRecommendedTaskCount()
    {
        return recommendedTaskCount;
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
                newChildren.stream().collect(onlyElement()),
                functionName,
                arguments,
                argumentColumns,
                outputVariable,
                streamingMode,
                dispatchBatchSize,
                recommendedTaskCount,
                executionPhase,
                inputGroupingExpressions);
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
                dispatchBatchSize,
                recommendedTaskCount,
                executionPhase,
                inputGroupingExpressions);
    }
}
