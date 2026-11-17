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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ExternalCallExpressionChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isRpcFunctionOptimizerEnabled;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

public class RewriteFilterWithExternalFunctionToProject
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FunctionAndTypeManager functionAndTypeManager;
    private final Supplier<Set<String>> rpcFunctionNames;

    public RewriteFilterWithExternalFunctionToProject(FunctionAndTypeManager functionAndTypeManager)
    {
        this(functionAndTypeManager, ImmutableSet::of);
    }

    /**
     * Rewriting a predicate into a projection is what lets a later optimizer move the call off the
     * critical path of the filter, so a function dispatched over RPC needs this rule as much as one
     * whose declared implementation type is external.
     *
     * @param rpcFunctionNames lower-case display names of functions a worker serves over RPC
     */
    public RewriteFilterWithExternalFunctionToProject(FunctionAndTypeManager functionAndTypeManager, Supplier<Set<String>> rpcFunctionNames)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.rpcFunctionNames = requireNonNull(rpcFunctionNames, "rpcFunctionNames is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        // The names are fetched once here rather than for every call the checker visits: fetching
        // them can mean rebuilding the set from the worker's function list. With RPC lifting
        // switched off, nothing would lift a call to one of them, so they are not treated as remote.
        Set<String> names = isRpcFunctionOptimizerEnabled(context.getSession()) ? rpcFunctionNames.get() : ImmutableSet.of();
        ExternalCallExpressionChecker checker = new ExternalCallExpressionChecker(functionAndTypeManager, names);
        if (!node.getPredicate().accept(checker, null)) {
            // No remote function in predicate
            return Result.empty();
        }

        // Only the conjuncts that call a remote function have to become a projection. The rest stay
        // in a filter below it, where predicate push down can still move them towards the table
        // scan. Lifting the whole conjunction instead would put the remaining predicate above the
        // call, so the call would be made for rows the query goes on to discard.
        ImmutableList.Builder<RowExpression> remoteConjuncts = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> localConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : extractConjuncts(node.getPredicate())) {
            if (conjunct.accept(checker, null)) {
                remoteConjuncts.add(conjunct);
            }
            else {
                localConjuncts.add(conjunct);
            }
        }
        List<RowExpression> localPredicates = localConjuncts.build();

        PlanNode source = node.getSource();
        if (!localPredicates.isEmpty()) {
            source = new FilterNode(
                    node.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    source,
                    and(localPredicates));
        }

        RowExpression remotePredicate = and(remoteConjuncts.build());
        VariableReferenceExpression predicateVariable = context.getVariableAllocator().newVariable(remotePredicate);
        Assignments.Builder assignments = Assignments.builder();
        node.getOutputVariables().forEach(variable -> assignments.put(variable, variable));
        Assignments identityAssignments = assignments.build();
        assignments.put(predicateVariable, remotePredicate);

        return Result.ofPlanNode(
                new ProjectNode(
                        node.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                            node.getSourceLocation(),
                            context.getIdAllocator().getNextId(),
                            new ProjectNode(context.getIdAllocator().getNextId(), source, assignments.build()),
                            predicateVariable),
                        identityAssignments,
                        LOCAL));
    }
}
