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
import com.facebook.presto.common.block.Block;
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
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isPullExpressionFromLambdaEnabled;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * If there are expressions in the body of a lambda function, which does not refer to the arguments of the lambda function, it can be
 * evaluated outside of the lambda function, hence avoid evaluating multiple times inside lambda body. An example of the optimization is:
 * Before:
 * <pre>
 *     - Project
 *          expr := filter(array, x -> x > id1+id2)
 *          - TableScan
 *              array: array(bigint)
 *              id1: bigint
 *              id2: bigint
 * </pre>
 * After:
 * <pre>
 *     - Project
 *          expr: filter(array, x -> x > sum)
 *          - Project:
 *              sum := id1+id2
 *              - TableScan
 *                  array: array(bigint)
 *                  id1: bigint
 *                  id2: bigint
 * </pre>
 */
public class PullUpExpressionInLambdaRules
{
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final FunctionResolution functionResolution;

    public PullUpExpressionInLambdaRules(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    private static Set<RowExpression> getCandidateRowExpression(RowExpressionDeterminismEvaluator determinismEvaluator, FunctionResolution functionResolution, List<VariableReferenceExpression> inputVariables,
            RowExpression rowExpression)
    {
        ImmutableSet.Builder<RowExpression> candidateBuilder = ImmutableSet.builder();
        ValidExpressionExtractor validCallExpressionExtractor = new ValidExpressionExtractor(determinismEvaluator, functionResolution, inputVariables, candidateBuilder);
        rowExpression.accept(validCallExpressionExtractor, false);
        // If row expression has no variable reference, i.e. is constant, do not pull out
        return candidateBuilder.build().stream().filter(x -> !extractAll(x).isEmpty()).collect(toImmutableSet());
    }

    public boolean isRuleEnabled(Session session)
    {
        return isPullExpressionFromLambdaEnabled(session);
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                filterNodeRule(),
                projectNodeRule());
    }

    public Rule<FilterNode> filterNodeRule()
    {
        return new PullUpExpressionInLambdaFilterNodeRule();
    }

    public Rule<ProjectNode> projectNodeRule()
    {
        return new PullUpExpressionInLambdaProjectNodeRule();
    }

    private final class PullUpExpressionInLambdaProjectNodeRule
            implements Rule<ProjectNode>
    {
        @Override
        public boolean isEnabled(Session session)
        {
            return isRuleEnabled(session);
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            List<VariableReferenceExpression> inputVariables = node.getSource().getOutputVariables();
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> pulledExpressionMapBuilder = ImmutableMap.builder();
            Assignments.Builder newProjectWithLambda = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                RowExpression rowExpression = entry.getValue();
                Set<RowExpression> candidates = getCandidateRowExpression(determinismEvaluator, functionResolution, inputVariables, rowExpression);
                if (candidates.isEmpty()) {
                    newProjectWithLambda.put(entry.getKey(), entry.getValue());
                }
                else {
                    Map<RowExpression, VariableReferenceExpression> mapping = candidates.stream().collect(toImmutableMap(identity(), x -> context.getVariableAllocator().newVariable(x)));
                    pulledExpressionMapBuilder.putAll(mapping.entrySet().stream().collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey)));
                    RowExpression rewrittenExpression = rowExpression.accept(new ExpressionRewriter(mapping), null);
                    newProjectWithLambda.put(entry.getKey(), rewrittenExpression);
                }
            }

            Map<VariableReferenceExpression, RowExpression> pulledExpressionMap = pulledExpressionMapBuilder.build();
            if (pulledExpressionMap.isEmpty()) {
                return Result.empty();
            }

            PlanNode planNode = PlannerUtils.addProjections(node.getSource(), context.getIdAllocator(), pulledExpressionMap);
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), planNode, newProjectWithLambda.build()));
        }
    }

    private final class PullUpExpressionInLambdaFilterNodeRule
            implements Rule<FilterNode>
    {
        @Override
        public boolean isEnabled(Session session)
        {
            return isRuleEnabled(session);
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            RowExpression predicate = filterNode.getPredicate();
            List<VariableReferenceExpression> inputVariables = filterNode.getSource().getOutputVariables();
            Set<RowExpression> candidates = getCandidateRowExpression(determinismEvaluator, functionResolution, inputVariables, predicate);
            if (candidates.isEmpty()) {
                return Result.empty();
            }
            Map<RowExpression, VariableReferenceExpression> mapping = candidates.stream().collect(toImmutableMap(identity(), x -> context.getVariableAllocator().newVariable(x)));
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> pulledExpressionMapBuilder = ImmutableMap.builder();
            pulledExpressionMapBuilder.putAll(mapping.entrySet().stream().collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey)));
            RowExpression rewrittenExpression = predicate.accept(new ExpressionRewriter(mapping), null);
            PlanNode planNode = PlannerUtils.addProjections(filterNode.getSource(), context.getIdAllocator(), pulledExpressionMapBuilder.build());
            return Result.ofPlanNode(
                    new ProjectNode(
                            context.getIdAllocator().getNextId(),
                            new FilterNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), planNode, rewrittenExpression),
                            identityAssignments(filterNode.getOutputVariables())));
        }
    }

    private static class ValidExpressionExtractor
            implements RowExpressionVisitor<Boolean, Boolean>
    {
        // Bind expression will complicate the lambda expression, we apply this optimization before DesugarLambdaRule. And if there are bind expression, skip
        private static final List<SpecialFormExpression.Form> UNSUPPORTED_TYPES = ImmutableList.of(SpecialFormExpression.Form.BIND);
        private static final List<Class<?>> SUPPORTED_JAVA_TYPES = ImmutableList.of(boolean.class, long.class, double.class, Slice.class, Block.class);
        private final RowExpressionDeterminismEvaluator determinismEvaluator;
        private final FunctionResolution functionResolution;
        private final List<VariableReferenceExpression> inputVariables;
        private final ImmutableSet.Builder<RowExpression> candidates;

        public ValidExpressionExtractor(RowExpressionDeterminismEvaluator determinismEvaluator,
                FunctionResolution functionResolution,
                List<VariableReferenceExpression> inputVariables,
                ImmutableSet.Builder<RowExpression> candidates)
        {
            this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.inputVariables = requireNonNull(inputVariables, "inputVariables is null");
            this.candidates = requireNonNull(candidates, "candidates is null");
        }

        @Override
        public Boolean visitCall(CallExpression call, Boolean context)
        {
            // Skip try function as pulling out function within try function can throw exception.
            // Skip subscript function as it can throw exception when pull out
            if (functionResolution.isTryFunction(call.getFunctionHandle()) || functionResolution.isSubscriptFunction(call.getFunctionHandle())) {
                return false;
            }
            Map<RowExpression, Boolean> validRowExpressionMap = call.getArguments().stream().distinct().collect(toImmutableMap(identity(), x -> x.accept(this, context)));
            if (context.equals(Boolean.TRUE)) {
                boolean allArgumentsValid = validRowExpressionMap.values().stream().allMatch(x -> x.equals(Boolean.TRUE));
                if (!allArgumentsValid) {
                    candidates.addAll(validRowExpressionMap.entrySet().stream()
                            .filter(x -> x.getValue().equals(Boolean.TRUE))
                            .map(Map.Entry::getKey)
                            .map(x -> getArgumentForRegexTypeExpression(x))
                            .filter(ValidExpressionExtractor::isSupportedExpression)
                            .collect(toImmutableList()));
                }
                return allArgumentsValid && determinismEvaluator.isDeterministic(call);
            }
            return false;
        }

        // For the conditional expressions, not all arguments will be evaluated, we only try to extract from the arguments which will always be executed
        private static List<RowExpression> getValidArguments(SpecialFormExpression specialForm)
        {
            List<RowExpression> validArgument;
            SpecialFormExpression.Form form = specialForm.getForm();
            if (form.equals(SpecialFormExpression.Form.IF) || form.equals(SpecialFormExpression.Form.COALESCE) || form.equals(SpecialFormExpression.Form.WHEN)) {
                validArgument = ImmutableList.of(specialForm.getArguments().get(0));
            }
            else if (form.equals(SpecialFormExpression.Form.SWITCH)) {
                validArgument = ImmutableList.of(specialForm.getArguments().get(0), specialForm.getArguments().get(1));
            }
            else {
                validArgument = specialForm.getArguments();
            }
            return validArgument;
        }

        // When expression cannot be pulled out, hence if we get a when expression, try to pull out its argument instead
        private static RowExpression getArgumentOfWhen(RowExpression expression)
        {
            if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm().equals(SpecialFormExpression.Form.WHEN)) {
                return getArgumentOfWhen(((SpecialFormExpression) expression).getArguments().get(0));
            }
            return expression;
        }

        // If the input is a CAST expression to cast to JoniRegexType or LikePatternType (underlying Java type is Regex.class) or is a like_pattern function, return the argument
        // Still return even if it's not a cast/like_pattern expression, as these types will be filtered by the isSupportedExpression later
        private RowExpression getArgumentForRegexTypeExpression(RowExpression rowExpression)
        {
            if (rowExpression.getType().getJavaType() == Regex.class && rowExpression instanceof CallExpression
                    && (functionResolution.isCastFunction(((CallExpression) rowExpression).getFunctionHandle())
                    || functionResolution.isLikePatternFunction(((CallExpression) rowExpression).getFunctionHandle()))) {
                CallExpression castExpression = (CallExpression) rowExpression;
                return getArgumentForRegexTypeExpression(castExpression.getArguments().get(0));
            }
            return rowExpression;
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, Boolean context)
        {
            if (UNSUPPORTED_TYPES.contains(specialForm.getForm())) {
                return false;
            }
            List<RowExpression> validArguments = getValidArguments(specialForm);
            Map<RowExpression, Boolean> validRowExpressionMap = specialForm.getArguments().stream().distinct().collect(toImmutableMap(identity(), x -> validArguments.contains(x) ? x.accept(this, context) : false));
            if (context.equals(Boolean.TRUE)) {
                boolean allArgumentsValid = validRowExpressionMap.values().stream().allMatch(x -> x.equals(Boolean.TRUE));
                if (!allArgumentsValid) {
                    candidates.addAll(validRowExpressionMap.entrySet().stream()
                            .filter(x -> x.getValue().equals(Boolean.TRUE))
                            .map(Map.Entry::getKey)
                            .map(ValidExpressionExtractor::getArgumentOfWhen)
                            .filter(ValidExpressionExtractor::isSupportedExpression)
                            .collect(toImmutableList()));
                }
                return allArgumentsValid && determinismEvaluator.isDeterministic(specialForm);
            }
            return false;
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression lambda, Boolean context)
        {
            if (lambda.getBody().accept(this, true) && isSupportedExpression(lambda.getBody())) {
                candidates.add(lambda.getBody());
            }
            // For simplicity, we do not pull out lambda expressions
            return false;
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, Boolean context)
        {
            return inputVariables.contains(reference);
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, Boolean context)
        {
            return true;
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression reference, Boolean context)
        {
            return false;
        }

        // WHEN expression should only exist within SWITCH expression, and will throw exception in RowExpressionInterpreter, also no byte code generator for standalone WHEN expression
        // Pull out LikePatternType and JoniRegexpType out can lead to byte code generation failure because of the underlying Regex type.
        private static boolean isSupportedExpression(RowExpression expression)
        {
            return (expression instanceof CallExpression || (expression instanceof SpecialFormExpression && !((SpecialFormExpression) expression).getForm().equals(SpecialFormExpression.Form.WHEN)))
                    && SUPPORTED_JAVA_TYPES.contains(expression.getType().getJavaType());
        }
    }

    private static class ExpressionRewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Map<RowExpression, VariableReferenceExpression> expressionMap;

        public ExpressionRewriter(Map<RowExpression, VariableReferenceExpression> expressionMap)
        {
            this.expressionMap = ImmutableMap.copyOf(expressionMap);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            List<RowExpression> rewrittenArguments = call.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList());
            RowExpression rewritten = new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    rewrittenArguments);
            if (expressionMap.containsKey(rewritten)) {
                return expressionMap.get(rewritten);
            }
            if (rowExpressionsNotChanged(call.getArguments(), rewrittenArguments)) {
                return call;
            }
            return rewritten;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(lambda.getSourceLocation(), lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            List<RowExpression> rewrittenArguments = specialForm.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList());
            SpecialFormExpression rewritten = new SpecialFormExpression(
                    specialForm.getForm(),
                    specialForm.getType(),
                    rewrittenArguments);
            if (expressionMap.containsKey(rewritten)) {
                return expressionMap.get(rewritten);
            }
            if (rowExpressionsNotChanged(specialForm.getArguments(), rewrittenArguments)) {
                return specialForm;
            }
            return rewritten;
        }

        private boolean rowExpressionsNotChanged(List<RowExpression> original, List<RowExpression> rewritten)
        {
            checkArgument(original.size() == rewritten.size());
            return IntStream.range(0, original.size()).boxed().allMatch(idx -> original.get(idx).equals(rewritten.get(idx)));
        }
    }
}
