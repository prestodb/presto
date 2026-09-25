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

import com.facebook.presto.Session;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isOptimizeCascadingFiltersAndProjections;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.isIdentity;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Merges an adjacent {@code Filter -> Project} by inlining the child projection's expressions into
 * the filter predicate and reordering to {@code Project -> Filter -> source}.
 *
 * <p>Concretely, {@code Filter(predicate, Project(assignments, source))} is rewritten to
 * {@code Project(assignments, Filter(predicate', source))}, where {@code predicate'} is the
 * predicate with the child's deterministic, non-identity assignments inlined so it is expressed
 * directly over the project's inputs. This co-locates the predicate and projections over the same
 * inputs, exposing shared subexpressions to the native (Velox) engine's common-subexpression
 * elimination and matching Velox's preferred filter-then-project (fused {@code FilterProject})
 * shape.
 *
 * <p>The project is retained because its outputs are still required upstream. The rule bails when
 * the predicate references a child output that is not safe to inline (non-deterministic and
 * referenced more than once, or an input to a {@code TRY(...)} expression) to preserve semantics.
 *
 * <p>The opposite arrangement ({@code Project} over {@code Filter}) is already in Velox's preferred
 * shape; any project above it is flattened by {@link CoalesceCascadingProjections}.
 *
 * <p>This rule is gated behind {@code optimize_cascading_filters_and_projections} and is disabled by
 * default.
 */
public class MergeFilterAndProjection
        implements Rule<FilterNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(project().capturedAs(CHILD)));

    private final FunctionResolution functionResolution;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public MergeFilterAndProjection(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeCascadingFiltersAndProjections(session);
    }

    @Override
    public Result apply(FilterNode filter, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        // Do not push below a remote project: the predicate would be evaluated in a different
        // locality than intended.
        if (child.getLocality().equals(REMOTE)) {
            return Result.empty();
        }

        RowExpression predicate = filter.getPredicate();
        Set<VariableReferenceExpression> childOutputSet = ImmutableSet.copyOf(child.getOutputVariables());

        // Count how many times each child output is referenced by the predicate and collect the
        // child outputs that feed a TRY(...) expression.
        Map<VariableReferenceExpression, Long> references = new HashMap<>();
        for (VariableReferenceExpression variable : extractAll(predicate)) {
            if (childOutputSet.contains(variable)) {
                references.merge(variable, 1L, Long::sum);
            }
        }
        Set<VariableReferenceExpression> tryArguments = extractTryArguments(predicate);

        // Determine the non-identity child outputs that must be inlined into the predicate.
        // Identity assignments (x := x) are passthroughs already produced by the source, so the
        // predicate can reference them directly once the filter sits below the project.
        Set<VariableReferenceExpression> inlineTargets = new HashSet<>();
        for (Map.Entry<VariableReferenceExpression, Long> entry : references.entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            if (isIdentity(child.getAssignments(), variable)) {
                continue;
            }
            RowExpression assignment = child.getAssignments().get(variable);
            // The project is kept (rebuilt above the new filter), so it still computes this
            // assignment. Inlining it into the predicate therefore duplicates the computation, which
            // is only safe for deterministic expressions; inlining a non-deterministic expression
            // would evaluate it independently in the filter and the projection (e.g. two different
            // random() draws), changing semantics. So require determinism regardless of how many
            // times the predicate references it.
            boolean inlinable = determinismEvaluator.isDeterministic(assignment);
            if (!inlinable || tryArguments.contains(variable)) {
                // The predicate references a child output that cannot be safely inlined; preserve
                // semantics by declining to fire.
                return Result.empty();
            }
            inlineTargets.add(variable);
        }

        if (inlineTargets.isEmpty()) {
            // Nothing to inline (predicate only references passthrough/identity outputs). Reordering
            // would not co-locate any computation, so leave the plan unchanged to avoid churn.
            return Result.empty();
        }

        Map<VariableReferenceExpression, RowExpression> inlineMap = new HashMap<>();
        for (VariableReferenceExpression variable : inlineTargets) {
            inlineMap.put(variable, child.getAssignments().get(variable));
        }
        RowExpression rewrittenPredicate = RowExpressionVariableInliner.inlineVariables(
                variable -> inlineMap.getOrDefault(variable, variable),
                predicate);

        FilterNode newFilter = new FilterNode(
                filter.getSourceLocation(),
                filter.getId(),
                child.getSource(),
                rewrittenPredicate);

        return Result.ofPlanNode(
                new ProjectNode(
                        child.getSourceLocation(),
                        child.getId(),
                        newFilter,
                        child.getAssignments(),
                        child.getLocality()));
    }

    private Set<VariableReferenceExpression> extractTryArguments(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>()
        {
            @Override
            public Void visitCall(CallExpression call, ImmutableSet.Builder<VariableReferenceExpression> context)
            {
                if (functionResolution.isTryFunction(call.getFunctionHandle())) {
                    context.addAll(extractAll(call));
                }
                return super.visitCall(call, context);
            }
        }, builder);
        return builder.build();
    }
}
