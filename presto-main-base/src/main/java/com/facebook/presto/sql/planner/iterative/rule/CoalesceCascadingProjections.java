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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.Assignments.Builder;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.ProjectNodeUtils;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isOptimizeCascadingFiltersAndProjections;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.isIdentity;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Coalesces a chain of {@code Project -> Project} into a single {@link ProjectNode} by inlining the
 * child projection's expressions into the parent, then dropping the residual child projection when it
 * collapses to a pure passthrough.
 *
 * <p>Like {@link InlineProjections}, this rule never duplicates real computation: a child output is
 * inlined only when it is referenced exactly once by the parent, or when its defining expression is
 * <em>trivial</em> to duplicate -- a constant (including a constant-folded {@code Block}) or a pure
 * variable rename. A non-trivial expression referenced more than once is left in the child, so the
 * chain is not collapsed at that output.
 *
 * <p>To preserve semantics this rule also skips inputs to {@code TRY(...)} expressions (inlining into
 * a TRY could change which errors are masked) and identity assignments (which would otherwise make
 * the rule fire forever). A single-use child output is safe to inline even when non-deterministic,
 * because a lone reference cannot duplicate the side-effecting computation.
 *
 * <p>This rule is gated behind {@code optimize_cascading_filters_and_projections} and is disabled by
 * default.
 */
public class CoalesceCascadingProjections
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(project().capturedAs(CHILD)));

    private final FunctionResolution functionResolution;

    public CoalesceCascadingProjections(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeCascadingFiltersAndProjections(session);
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        // Do not inline remote projections, or if parent and child have different locality
        if (parent.getLocality().equals(REMOTE) || child.getLocality().equals(REMOTE) || !parent.getLocality().equals(child.getLocality())) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> targets = extractInliningTargets(parent, child);
        if (targets.isEmpty()) {
            return Result.empty();
        }

        // inline the expressions
        Assignments assignments = child.getAssignments().filter(targets::contains);
        Set<VariableReferenceExpression> targetVars = assignments.getVariables();
        Map<VariableReferenceExpression, RowExpression> parentAssignments = parent.getAssignments()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            RowExpression expression = entry.getValue();
                            // Skip inlining for simple variable references that aren't targets.
                            // For wide projections, the majority of assignments are passthrough
                            // variables that don't reference any inlining target.
                            if (expression instanceof VariableReferenceExpression
                                    && !targetVars.contains(expression)) {
                                return expression;
                            }
                            return inlineReferences(expression, assignments);
                        }));

        // Synthesize identity assignments for the inputs of expressions that were inlined
        // to place in the child projection.
        // If all assignments end up becoming identity assignments, they'll get pruned by
        // other rules
        Set<VariableReferenceExpression> inputs = child.getAssignments()
                .entrySet().stream()
                .filter(entry -> targets.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(expression -> extractAll(expression).stream())
                .collect(toSet());

        Builder childAssignments = Assignments.builder();
        Set<VariableReferenceExpression> retainedChildOutputs = new HashSet<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : child.getAssignments().entrySet()) {
            if (!targets.contains(assignment.getKey())) {
                childAssignments.put(assignment);
                retainedChildOutputs.add(assignment.getKey());
            }
        }
        // Only synthesize identity passthroughs for inputs that a retained child assignment does not
        // already produce. Variables are globally unique, so an input (a grandchild output) can only
        // coincide with a retained child output in the identity-passthrough case (x := x); guarding
        // here keeps the rule strictly additive and never overwrites a retained assignment.
        inputs.stream()
                .filter(input -> !retainedChildOutputs.contains(input))
                .forEach(input -> childAssignments.put(input, input));

        ProjectNode newChild = new ProjectNode(
                parent.getSourceLocation(),
                child.getId(),
                child.getSource(),
                childAssignments.build(),
                child.getLocality());

        // If the residual child collapsed to a pure passthrough (all identity assignments), drop it
        // and attach the parent directly to the grandchild. The parent's inlined expressions already
        // reference the grandchild's outputs, so the intermediate identity projection is redundant.
        // This avoids leaving an empty/narrowing identity ProjectNode for later passes to clean up.
        PlanNode newSource = ProjectNodeUtils.isIdentity(newChild) ? child.getSource() : newChild;

        return Result.ofPlanNode(
                new ProjectNode(
                        parent.getSourceLocation(),
                        parent.getId(),
                        newSource,
                        Assignments.copyOf(parentAssignments),
                        parent.getLocality()));
    }

    private RowExpression inlineReferences(RowExpression expression, Assignments assignments)
    {
        return RowExpressionVariableInliner.inlineVariables(variable -> assignments.getMap().getOrDefault(variable, variable), expression);
    }

    private Set<VariableReferenceExpression> extractInliningTargets(ProjectNode parent, ProjectNode child)
    {
        // candidates for inlining are child outputs that are referenced by the parent and
        //   a. are not inputs to try() expressions (otherwise inlining might change semantics)
        //   b. are not identity projections (otherwise this rule would keep firing forever)
        //   c. are either referenced exactly once (a single reference never duplicates computation,
        //      even for a non-deterministic expression) OR are trivial to duplicate -- a constant or
        //      a pure variable rename (see isTrivial)
        // Like InlineProjections, this rule never inlines a multiply-referenced non-trivial
        // expression, so it does not duplicate real computation across the coalesced projection.

        Set<VariableReferenceExpression> childOutputSet = ImmutableSet.copyOf(child.getOutputVariables());

        // Single pass over parent expressions: count variable references and collect TRY arguments
        Map<VariableReferenceExpression, Long> dependencies = new HashMap<>();
        Set<VariableReferenceExpression> tryArguments = new HashSet<>();
        for (RowExpression expression : parent.getAssignments().getExpressions()) {
            for (VariableReferenceExpression variable : extractAll(expression)) {
                if (childOutputSet.contains(variable)) {
                    dependencies.merge(variable, 1L, Long::sum);
                }
            }
            tryArguments.addAll(extractTryArguments(expression));
        }

        return dependencies.entrySet().stream()
                .filter(entry -> !tryArguments.contains(entry.getKey()))
                .filter(entry -> !isIdentity(child.getAssignments(), entry.getKey()))
                .filter(entry -> entry.getValue() == 1 || isTrivial(child.getAssignments().get(entry.getKey())))
                .map(Map.Entry::getKey)
                .collect(toSet());
    }

    /**
     * A trivial expression is free to duplicate, so it may be inlined even when the child output is
     * referenced more than once: a constant (including a constant-folded {@code Block}) or a pure
     * variable rename. Inlining these never duplicates real computation.
     */
    private static boolean isTrivial(RowExpression expression)
    {
        return expression instanceof ConstantExpression || expression instanceof VariableReferenceExpression;
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
