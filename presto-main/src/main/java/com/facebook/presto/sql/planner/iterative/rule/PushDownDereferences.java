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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.Rule.Context;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.unnest;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.ExpressionTreeRewriter.rewriteWith;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Push down dereferences as follows:
 * <p>
 * Extract dereferences from PlanNode which has expressions
 * and push them down to a new ProjectNode right below the PlanNode.
 * After this step, All dereferences will be in ProjectNode.
 * <p>
 * Pushdown dereferences in ProjectNode down through other types of PlanNode,
 * e.g, Filter, Join etc.
 */
public class PushDownDereferences
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushDownDereferences(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ExtractFromFilter(),
                new ExtractFromJoin(),
                new PushDownDereferenceThrough<>(AssignUniqueId.class),
                new PushDownDereferenceThrough<>(WindowNode.class),
                new PushDownDereferenceThrough<>(TopNNode.class),
                new PushDownDereferenceThrough<>(RowNumberNode.class),
                new PushDownDereferenceThrough<>(SortNode.class),
                new PushDownDereferenceThrough<>(FilterNode.class),
                new PushDownDereferenceThrough<>(LimitNode.class),
                new PushDownDereferenceThroughProject(),
                new PushDownDereferenceThroughUnnest(),
                new PushDownDereferenceThroughSemiJoin(),
                new PushDownDereferenceThroughJoin());
    }

    /**
     * Extract dereferences and push them down to new ProjectNode below
     * Transforms:
     * <pre>
     *  TargetNode(expression(a.x))
     *  </pre>
     * to:
     * <pre>
     *   ProjectNode(original symbols)
     *    TargetNode(expression(symbol))
     *      Project(symbol := a.x)
     * </pre>
     */
    abstract class ExtractProjectDereferences<N extends PlanNode>
            implements Rule<N>
    {
        private final Class<N> planNodeClass;

        ExtractProjectDereferences(Class<N> planNodeClass)
        {
            this.planNodeClass = planNodeClass;
        }

        @Override
        public Pattern<N> getPattern()
        {
            return Pattern.typeOf(planNodeClass);
        }

        @Override
        public Result apply(N node, Captures captures, Context context)
        {
            Map<DereferenceExpression, VariableReferenceExpression> expressions =
                    getDereferenceSymbolMap(extractExpressionsNonRecursive(node), context, metadata, sqlParser);

            if (expressions.isEmpty()) {
                return Result.empty();
            }

            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), rewrite(context, node, HashBiMap.create(expressions)), identityAssignmentsAsSymbolReferences(node.getOutputVariables())));
        }

        protected abstract N rewrite(Context context, N node, BiMap<DereferenceExpression, VariableReferenceExpression> expressions);
    }

    class ExtractFromFilter
            extends ExtractProjectDereferences<FilterNode>
    {
        ExtractFromFilter()
        {
            super(FilterNode.class);
        }

        @Override
        protected FilterNode rewrite(Context context, FilterNode node, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            PlanNode source = node.getSource();

            Map<VariableReferenceExpression, RowExpression> dereferencesMap = expressions.inverse().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())));
            Assignments assignments = Assignments.builder()
                    .putAll(identityAssignmentsAsSymbolReferences(source.getOutputVariables()))
                    .putAll(dereferencesMap)
                    .build();
            ProjectNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(), source, assignments);
            return new FilterNode(
                    context.getIdAllocator().getNextId(),
                    projectNode,
                    replaceDereferences(node.getPredicate(), expressions));
        }
    }

    class ExtractFromJoin
            extends ExtractProjectDereferences<JoinNode>
    {
        ExtractFromJoin()
        {
            super(JoinNode.class);
        }

        @Override
        protected JoinNode rewrite(Context context, JoinNode joinNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            Assignments.Builder leftSideDereferences = Assignments.builder();
            Assignments.Builder rightSideDereferences = Assignments.builder();

            for (Map.Entry<VariableReferenceExpression, DereferenceExpression> entry : expressions.inverse().entrySet()) {
                VariableReferenceExpression baseSymbol = getBase(entry.getValue(), context.getVariableAllocator().getTypes());
                if (joinNode.getLeft().getOutputVariables().contains(baseSymbol)) {
                    leftSideDereferences.put(entry.getKey(), castToRowExpression(entry.getValue()));
                }
                else {
                    rightSideDereferences.put(entry.getKey(), castToRowExpression(entry.getValue()));
                }
            }
            PlanNode leftNode = createProject(joinNode.getLeft(), leftSideDereferences.build(), context.getIdAllocator());
            PlanNode rightNode = createProject(joinNode.getRight(), rightSideDereferences.build(), context.getIdAllocator());

            return new JoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    joinNode.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(leftNode.getOutputVariables())
                            .addAll(rightNode.getOutputVariables())
                            .build(),
                    joinNode.getFilter().map(expression -> replaceDereferences(expression, expressions)),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType());
        }
    }

    /**
     * Push down dereferences from ProjectNode to child nodes if possible
     */
    private abstract class PushdownDereferencesInProject<N extends PlanNode>
            implements Rule<ProjectNode>
    {
        private final Capture<N> targetCapture = newCapture();
        private final Pattern<N> targetPattern;

        protected PushdownDereferencesInProject(Pattern<N> targetPattern)
        {
            this.targetPattern = requireNonNull(targetPattern, "targetPattern is null");
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(targetPattern.capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            N child = captures.get(targetCapture);
            Map<DereferenceExpression, VariableReferenceExpression> allDereferencesInProject = getDereferenceSymbolMap(node.getAssignments().getExpressions(), context, metadata, sqlParser);

            Set<VariableReferenceExpression> childSourceSymbols = child.getSources().stream()
                    .map(PlanNode::getOutputVariables).flatMap(Collection::stream)
                    .collect(toImmutableSet());

            Map<DereferenceExpression, VariableReferenceExpression> pushdownDereferences = allDereferencesInProject.entrySet().stream()
                    .filter(entry -> childSourceSymbols.contains(getBase(entry.getKey(), context.getVariableAllocator().getTypes())))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (pushdownDereferences.isEmpty()) {
                return Result.empty();
            }

            Result result = pushDownDereferences(context, child, HashBiMap.create(pushdownDereferences));
            if (result.isEmpty()) {
                return Result.empty();
            }

            Assignments.Builder builder = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                if (OriginalExpressionUtils.isExpression(entry.getValue())) {
                    builder.put(entry.getKey(), replaceDereferences(entry.getValue(), pushdownDereferences));
                }
                else {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), result.getTransformedPlan().get(), builder.build()));
        }

        protected abstract Result pushDownDereferences(Context context, N targetNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions);
    }

    /**
     * Transforms:
     * <pre>
     *  Project(a_x := a.x)
     *    TargetNode(a)
     *  </pre>
     * to:
     * <pre>
     *  Project(a_x := symbol)
     *    TargetNode(symbol)
     *      Project(symbol := a.x)
     * </pre>
     */
    public class PushDownDereferenceThrough<N extends PlanNode>
            extends PushdownDereferencesInProject<N>
    {
        public PushDownDereferenceThrough(Class<N> planNodeClass)
        {
            super(Pattern.typeOf(planNodeClass));
        }

        @Override
        protected Result pushDownDereferences(Context context, N targetNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            PlanNode source = getOnlyElement(targetNode.getSources());

            Map<VariableReferenceExpression, RowExpression> dereferencesMap =
                    expressions.inverse().entrySet().stream()
                            .collect(toImmutableMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())));
            ProjectNode projectNode = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    source,
                    Assignments.builder()
                            .putAll(identityAssignmentsAsSymbolReferences(source.getOutputVariables()))
                            .putAll(dereferencesMap)
                            .build());
            return Result.ofPlanNode(targetNode.replaceChildren(ImmutableList.of(projectNode)));
        }
    }

    /**
     * Transforms:
     * <pre>
     *  Project(a_x := a.msg.x)
     *    Join(a_y = b_y) => [a]
     *      Project(a_y := a.msg.y)
     *          Source(a)
     *      Project(b_y := b.msg.y)
     *          Source(b)
     *  </pre>
     * to:
     * <pre>
     *  Project(a_x := symbol)
     *    Join(a_y = b_y) => [symbol]
     *      Project(symbol := a.msg.x, a_y := a.msg.y)
     *        Source(a)
     *      Project(b_y := b.msg.y)
     *        Source(b)
     * </pre>
     */
    public class PushDownDereferenceThroughJoin
            extends PushdownDereferencesInProject<JoinNode>
    {
        PushDownDereferenceThroughJoin()
        {
            super(join());
        }

        @Override
        protected Result pushDownDereferences(Context context, JoinNode joinNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            Assignments.Builder leftSideDereferences = Assignments.builder();
            Assignments.Builder rightSideDereferences = Assignments.builder();

            for (Map.Entry<VariableReferenceExpression, DereferenceExpression> entry : expressions.inverse().entrySet()) {
                VariableReferenceExpression baseSymbol = getBase(entry.getValue(), context.getVariableAllocator().getTypes());
                if (joinNode.getLeft().getOutputVariables().contains(baseSymbol)) {
                    leftSideDereferences.put(entry.getKey(), castToRowExpression(entry.getValue()));
                }
                else {
                    rightSideDereferences.put(entry.getKey(), castToRowExpression(entry.getValue()));
                }
            }
            PlanNode leftNode = createProject(joinNode.getLeft(), leftSideDereferences.build(), context.getIdAllocator());
            PlanNode rightNode = createProject(joinNode.getRight(), rightSideDereferences.build(), context.getIdAllocator());

            return Result.ofPlanNode(new JoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    joinNode.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(leftNode.getOutputVariables())
                            .addAll(rightNode.getOutputVariables())
                            .build(),
                    joinNode.getFilter(),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType()));
        }
    }

    public class PushDownDereferenceThroughSemiJoin
            extends PushdownDereferencesInProject<SemiJoinNode>
    {
        PushDownDereferenceThroughSemiJoin()
        {
            super(semiJoin());
        }

        @Override
        protected Result pushDownDereferences(Context context, SemiJoinNode semiJoinNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            Assignments.Builder filteringSourceDereferences = Assignments.builder();
            Assignments.Builder sourceDereferences = Assignments.builder();

            for (Map.Entry<VariableReferenceExpression, DereferenceExpression> entry : expressions.inverse().entrySet()) {
                VariableReferenceExpression baseSymbol = getBase(entry.getValue(), context.getVariableAllocator().getTypes());
                if (semiJoinNode.getFilteringSource().getOutputVariables().contains(baseSymbol)) {
                    filteringSourceDereferences.put(entry.getKey(), castToRowExpression(entry.getValue()));
                }
                else {
                    sourceDereferences.put(entry.getKey(), castToRowExpression(entry.getValue()));
                }
            }
            PlanNode filteringSource = createProject(semiJoinNode.getFilteringSource(), filteringSourceDereferences.build(), context.getIdAllocator());
            PlanNode source = createProject(semiJoinNode.getSource(), sourceDereferences.build(), context.getIdAllocator());
            return Result.ofPlanNode(semiJoinNode.replaceChildren(ImmutableList.of(source, filteringSource)));
        }
    }

    public class PushDownDereferenceThroughProject
            extends PushdownDereferencesInProject<ProjectNode>
    {
        PushDownDereferenceThroughProject()
        {
            super(project());
        }

        @Override
        protected Result pushDownDereferences(Context context, ProjectNode projectNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            Map<VariableReferenceExpression, RowExpression> dereferencesMap = expressions.inverse().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())));

            return Result.ofPlanNode(
                    new ProjectNode(context.getIdAllocator().getNextId(),
                            projectNode.getSource(),
                            Assignments.builder()
                                    .putAll(projectNode.getAssignments())
                                    .putAll(dereferencesMap)
                                    .build()));
        }
    }

    public class PushDownDereferenceThroughUnnest
            extends PushdownDereferencesInProject<UnnestNode>
    {
        PushDownDereferenceThroughUnnest()
        {
            super(unnest());
        }

        @Override
        protected Result pushDownDereferences(Context context, UnnestNode unnestNode, BiMap<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            Map<VariableReferenceExpression, RowExpression> dereferencesMap = expressions.inverse().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> castToRowExpression(entry.getValue())));
            Assignments assignments = Assignments.builder()
                    .putAll(identityAssignmentsAsSymbolReferences(unnestNode.getSource().getOutputVariables()))
                    .putAll(dereferencesMap)
                    .build();
            ProjectNode source = new ProjectNode(context.getIdAllocator().getNextId(), unnestNode.getSource(), assignments);

            return Result.ofPlanNode(
                    new UnnestNode(context.getIdAllocator().getNextId(),
                            source,
                            ImmutableList.<VariableReferenceExpression>builder()
                                    .addAll(unnestNode.getReplicateVariables())
                                    .addAll(expressions.values())
                                    .build(),
                            unnestNode.getUnnestVariables(),
                            unnestNode.getOrdinalityVariable()));
        }
    }

    private RowExpression replaceDereferences(RowExpression rowExpression, Map<DereferenceExpression, VariableReferenceExpression> dereferences)
    {
        return castToRowExpression(rewriteWith(new DereferenceReplacer(dereferences), castToExpression(rowExpression)));
    }

    private static PlanNode createProject(PlanNode planNode, Assignments dereferences, PlanNodeIdAllocator idAllocator)
    {
        if (dereferences.isEmpty()) {
            return planNode;
        }
        Assignments assignments = Assignments.builder()
                .putAll(identityAssignmentsAsSymbolReferences(planNode.getOutputVariables()))
                .putAll(dereferences)
                .build();
        return new ProjectNode(idAllocator.getNextId(), planNode, assignments);
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<DereferenceExpression, VariableReferenceExpression> expressions;

        DereferenceReplacer(Map<DereferenceExpression, VariableReferenceExpression> expressions)
        {
            this.expressions = requireNonNull(expressions, "expressions is null");
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (expressions.containsKey(node)) {
                return new SymbolReference(expressions.get(node).getName());
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static List<DereferenceExpression> extractDereferenceExpressions(Expression expression)
    {
        ImmutableList.Builder<DereferenceExpression> builder = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<DereferenceExpression>>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<DereferenceExpression> context)
            {
                if (validPushDown(node)) {
                    context.add(node);
                }
                return null;
            }
        }.process(expression, builder);
        return builder.build();
    }

    private static Map<DereferenceExpression, VariableReferenceExpression> getDereferenceSymbolMap(Collection<RowExpression> expressions, Context context, Metadata metadata, SqlParser sqlParser)
    {
        Set<DereferenceExpression> dereferences = expressions.stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .flatMap(expression -> extractDereferenceExpressions(expression).stream())
                .collect(toImmutableSet());

        // DereferenceExpression with the same base will cause unnecessary rewritten
        if (dereferences.stream().anyMatch(expression -> baseExists(expression, dereferences))) {
            return ImmutableMap.of();
        }

        return dereferences.stream()
                .collect(toImmutableMap(Function.identity(), expression -> newSymbol(expression, context, metadata, sqlParser)));
    }

    private static VariableReferenceExpression newSymbol(Expression expression, Context context, Metadata metadata, SqlParser sqlParser)
    {
        Type type = getExpressionTypes(context.getSession(), metadata, sqlParser, context.getVariableAllocator().getTypes(), expression, emptyList(), WarningCollector.NOOP).get(NodeRef.of(expression));
        verify(type != null);
        return context.getVariableAllocator().newVariable(expression, type);
    }

    private static boolean baseExists(DereferenceExpression expression, Set<DereferenceExpression> dereferences)
    {
        Expression base = expression.getBase();
        while (base instanceof DereferenceExpression) {
            if (dereferences.contains(base)) {
                return true;
            }
            base = ((DereferenceExpression) base).getBase();
        }
        return false;
    }

    private static boolean validPushDown(DereferenceExpression dereference)
    {
        Expression base = dereference.getBase();
        return (base instanceof SymbolReference) || (base instanceof DereferenceExpression);
    }

    private static VariableReferenceExpression getBase(Expression expression, TypeProvider types)
    {
        return getOnlyElement(extractAll(expression, types));
    }
}
