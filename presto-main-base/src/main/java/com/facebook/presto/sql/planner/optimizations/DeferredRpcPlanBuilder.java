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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getRpcDeferredCollectTaskCount;
import static com.facebook.presto.SystemSessionProperties.getRpcDeferredPollTaskCount;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class DeferredRpcPlanBuilder
{
    private DeferredRpcPlanBuilder() {}

    public static PlanNode build(
            Optional<SourceLocation> location,
            PlanNode source,
            VariableReferenceExpression result,
            Set<VariableReferenceExpression> requiredPassthrough,
            DeferredRpcExecution execution,
            Session session,
            VariableAllocator variables,
            PlanNodeIdAllocator ids,
            FunctionAndTypeManager functionAndTypeManager)
    {
        DeferredRpcCall submitCall = requireNonNull(execution.submit(), "submit call is null");
        checkArgument(submitCall.getInputGroupingExpressions().isEmpty(), "Deferred submit input grouping is not supported");
        VariableReferenceExpression receipt = variables.newVariable("__rpc_receipt", submitCall.getOutputType());
        PlanNode submit = phase(location, source, submitCall, receipt, RPCNode.ExecutionPhase.SUBMIT, 0, variables, ids, functionAndTypeManager);

        DeferredRpcCall pollCall = requireNonNull(execution.poll(receipt), "poll call is null");
        VariableReferenceExpression ready = variables.newVariable("__rpc_ready", pollCall.getOutputType());
        DeferredRpcCall collectCall = requireNonNull(execution.collect(ready), "collect call is null");
        checkArgument(collectCall.getOutputType().equals(result.getType()), "Deferred collect result type does not match the original RPC result");
        ImmutableSet.Builder<VariableReferenceExpression> collectDependencies = ImmutableSet.builder();
        collectCall.getArguments().forEach(argument -> collectDependencies.addAll(extractUnique(argument)));
        collectCall.getInputGroupingExpressions().forEach(expression -> collectDependencies.addAll(extractUnique(expression)));
        Set<VariableReferenceExpression> preservedForCollect = collectDependencies.build().stream()
                .filter(variable -> !variable.equals(ready)).collect(ImmutableSet.toImmutableSet());
        checkArgument(submit.getOutputVariables().containsAll(preservedForCollect), "Deferred collect references unavailable input variables");
        Set<VariableReferenceExpression> pollPassthrough = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(requiredPassthrough).addAll(preservedForCollect).build();
        PlanNode pollSource = projectInputs(location, submit, pollPassthrough, pollCall, receipt, ids);
        PlanNode poll = phase(location, pollSource, pollCall, ready, RPCNode.ExecutionPhase.POLL,
                getRpcDeferredPollTaskCount(session), variables, ids, functionAndTypeManager);

        PlanNode collectSource = projectInputs(location, poll, requiredPassthrough, collectCall, ready, ids);
        return phase(location, collectSource, collectCall, result, RPCNode.ExecutionPhase.COLLECT,
                getRpcDeferredCollectTaskCount(session), variables, ids, functionAndTypeManager);
    }

    private static PlanNode projectInputs(
            Optional<SourceLocation> location,
            PlanNode source,
            Set<VariableReferenceExpression> requiredPassthrough,
            DeferredRpcCall call,
            VariableReferenceExpression receipt,
            PlanNodeIdAllocator ids)
    {
        ImmutableSet.Builder<VariableReferenceExpression> inputs = ImmutableSet.builder();
        inputs.add(receipt);
        call.getArguments().forEach(argument -> inputs.addAll(extractUnique(argument)));
        call.getInputGroupingExpressions().forEach(expression -> inputs.addAll(extractUnique(expression)));
        Set<VariableReferenceExpression> dependencies = inputs.build();
        checkArgument(source.getOutputVariables().containsAll(dependencies), "Deferred phase references unavailable input variables");
        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression variable : source.getOutputVariables()) {
            if (requiredPassthrough.contains(variable) || dependencies.contains(variable)) {
                assignments.put(variable, variable);
            }
        }
        return new ProjectNode(location, ids.getNextId(), source, assignments.build(), ProjectNode.Locality.LOCAL);
    }

    private static PlanNode phase(
            Optional<SourceLocation> location,
            PlanNode source,
            DeferredRpcCall call,
            VariableReferenceExpression result,
            RPCNode.ExecutionPhase phase,
            int taskCount,
            VariableAllocator variables,
            PlanNodeIdAllocator ids,
            FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(phase == RPCNode.ExecutionPhase.SUBMIT || !call.getInputGroupingExpressions().isEmpty(),
                "Deferred poll and collect calls must declare input grouping");
        for (RowExpression expression : call.getInputGroupingExpressions()) {
            checkArgument(expression.getType().isComparable(), "Deferred grouping type is not hashable");
            checkArgument(source.getOutputVariables().containsAll(extractUnique(expression)), "Deferred grouping references unavailable variables");
            if (functionAndTypeManager != null) {
                checkArgument(new RowExpressionDeterminismEvaluator(functionAndTypeManager).isDeterministic(expression),
                        "Deferred grouping must be deterministic");
            }
        }
        Assignments.Builder assignments = Assignments.builder();
        source.getOutputVariables().forEach(variable -> assignments.put(variable, variable));
        ImmutableList.Builder<String> columns = ImmutableList.builder();
        boolean project = false;
        for (RowExpression argument : call.getArguments()) {
            checkArgument(source.getOutputVariables().containsAll(extractUnique(argument)), "Deferred argument references unavailable variables");
            if (argument instanceof VariableReferenceExpression) {
                columns.add(((VariableReferenceExpression) argument).getName());
            }
            else {
                VariableReferenceExpression variable = variables.newVariable("__rpc_arg", argument.getType());
                columns.add(variable.getName());
                if (phase == RPCNode.ExecutionPhase.SUBMIT || !(argument instanceof ConstantExpression)) {
                    assignments.put(variable, argument);
                    project = true;
                }
            }
        }
        if (project) {
            source = new ProjectNode(location, ids.getNextId(), source, assignments.build(), ProjectNode.Locality.LOCAL);
        }
        return new RPCNode(location, ids.getNextId(), Optional.empty(), source, call.getFunctionName(),
                call.getArguments(), columns.build(), result, RPCNode.StreamingMode.BATCH, call.getDispatchBatchSize(),
                taskCount, phase, call.getInputGroupingExpressions());
    }
}
