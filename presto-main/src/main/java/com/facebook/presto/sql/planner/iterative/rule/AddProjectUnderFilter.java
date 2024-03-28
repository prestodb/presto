package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.gen.CommonSubExpressionRewriter;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isProjectCommonExpressionInFilterAndProjectEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class AddProjectUnderFilter
        implements Rule<ProjectNode>
{
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private static final Capture<FilterNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(filter().capturedAs(CHILD)));

    public AddProjectUnderFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isProjectCommonExpressionInFilterAndProjectEnabled(session);
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        List<RowExpression> assignmentExpressions = node.getAssignments().getExpressions().stream().filter(
                x -> !(x instanceof VariableReferenceExpression) && isExpensiveExpression(x) && determinismEvaluator.isDeterministic(x)).collect(toImmutableList());
        FilterNode filterNode = captures.get(CHILD);
        RowExpression filterExpression = filterNode.getPredicate();
        for (RowExpression assignment : assignmentExpressions) {
            CommonSubExpressionRewriter.SubExpressionChecker subExpressionChecker = new CommonSubExpressionRewriter.SubExpressionChecker(ImmutableSet.of(assignment));
            if (filterExpression.accept(subExpressionChecker, null)) {
                VariableReferenceExpression newVariable = context.getVariableAllocator().newVariable(assignment);
                CommonSubExpressionRewriter.ExpressionRewriter expressionRewriter = new CommonSubExpressionRewriter.ExpressionRewriter(ImmutableMap.of(assignment, newVariable));
                RowExpression newFilter = filterExpression.accept(expressionRewriter, null);
                PlanNode projectSource = addProjections(filterNode.getSource(), context.getIdAllocator(), ImmutableMap.of(newVariable, assignment));
                FilterNode newFilterNode = new FilterNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), filterNode.getStatsEquivalentPlanNode(), projectSource, newFilter);
                Assignments.Builder newAssignments = Assignments.builder();
                newAssignments.putAll(node.getAssignments().getMap().entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().equals(assignment) ? newVariable : entry.getValue())));
                return Result.ofPlanNode(new ProjectNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), node.getStatsEquivalentPlanNode(), newFilterNode, newAssignments.build(), node.getLocality()));
            }
        }
        return Result.empty();
    }

    private boolean isExpensiveExpression(RowExpression expression)
    {
        return expression.accept(new ExpensiveExpressionChecker(), null);
    }

    // Mark as expensive if the expression has lambda or involves array or map types
    public static class ExpensiveExpressionChecker
            implements RowExpressionVisitor<Boolean, Void>
    {
        @Override
        public Boolean visitCall(CallExpression call, Void context)
        {
            return call.getArguments().stream().anyMatch(expression -> expression.accept(this, null));
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression reference, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference.getType() instanceof ArrayType || reference.getType() instanceof MapType;
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return specialForm.getArguments().stream().anyMatch(expression -> expression.accept(this, null));
        }
    }
}
