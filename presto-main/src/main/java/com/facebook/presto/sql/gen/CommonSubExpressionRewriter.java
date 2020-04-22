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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.NULL_IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.relational.Expressions.subExpressions;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class CommonSubExpressionRewriter
{
    private CommonSubExpressionRewriter() {}

    public static class SubExpressionInfo
    {
        private final VariableReferenceExpression variable;
        private final int occurrence;

        public SubExpressionInfo(VariableReferenceExpression variable, int occurrence)
        {
            this.variable = requireNonNull(variable, "variable is null");
            this.occurrence = occurrence;
        }

        public VariableReferenceExpression getVariable()
        {
            return this.variable;
        }

        public int getOccurrence()
        {
            return occurrence;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            SubExpressionInfo other = (SubExpressionInfo) obj;
            return Objects.equals(this.variable, other.variable)
                    && this.occurrence == other.occurrence;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(variable, occurrence);
        }

        @Override
        public String toString()
        {
            return format("(%s: %d)", variable, occurrence);
        }
    }

    public static class CommonSubExpressionState
    {
        private final Variable resultVariable;
        private final LabelNode evalLabel;
        private final Variable instanceVariable;
        private final List<LabelNode> instanceLabels;
        private int occurrence;

        public CommonSubExpressionState(Variable resultVariable, LabelNode evalLabel, Variable instanceVariable, List<LabelNode> instanceLabels)
        {
            this.resultVariable = resultVariable;
            this.evalLabel = evalLabel;
            this.instanceVariable = instanceVariable;
            this.instanceLabels = instanceLabels;
        }
        public void calcuateCommonSubExpression(BytecodeBlock block)
        {
            block.append(instanceVariable.set(constantInt(occurrence)))
                    .gotoLabel(evalLabel)
                    .visitLabel(instanceLabels.get(occurrence));
            this.occurrence += 1;
        }
        public void appendResultVariable(BytecodeBlock block)
        {
            block.append(resultVariable);
        }
    }

    public static Map<Integer, Map<RowExpression, SubExpressionInfo>> collectCSEByLevel(List<? extends RowExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return ImmutableMap.of();
        }

        CommonSubExpressionCollector expressionCollector = new CommonSubExpressionCollector();
        expressions.forEach(expression -> expression.accept(expressionCollector, true));
        if (expressionCollector.cseByLevel.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Integer, Map<RowExpression, Integer>> cseByLevel = removeRedundantCSE(expressionCollector.cseByLevel, expressionCollector.expressionCount);

        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        ImmutableMap.Builder<Integer, Map<RowExpression, SubExpressionInfo>> commonSubExpressions = ImmutableMap.builder();
        Map<RowExpression, VariableReferenceExpression> rewriteWith = new HashMap<>();
        int startCSELevel = cseByLevel.keySet().stream().reduce(Math::min).get();
        int maxCSELevel = cseByLevel.keySet().stream().reduce(Math::max).get();
        for (int i = startCSELevel; i <= maxCSELevel; i++) {
            if (cseByLevel.containsKey(i)) {
                ExpressionRewritter rewritter = new ExpressionRewritter(rewriteWith);
                ImmutableMap.Builder<RowExpression, SubExpressionInfo> expressionVariableMapBuilder = ImmutableMap.builder();
                for (Map.Entry<RowExpression, Integer> entry : cseByLevel.get(i).entrySet()) {
                    RowExpression rewrittenExpression = entry.getKey().accept(rewritter, null);
                    expressionVariableMapBuilder.put(rewrittenExpression, new SubExpressionInfo(variableAllocator.newVariable(rewrittenExpression, "cse"), entry.getValue()));
                }
                Map<RowExpression, SubExpressionInfo> expressionVariableMap = expressionVariableMapBuilder.build();
                commonSubExpressions.put(i, expressionVariableMap);
                rewriteWith.putAll(expressionVariableMap.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().variable)));
            }
        }
        return commonSubExpressions.build();
    }

    public static Map<Integer, Map<RowExpression, SubExpressionInfo>> collectCSEByLevel(RowExpression expression)
    {
        return collectCSEByLevel(ImmutableList.of(expression));
    }

    public static List<RowExpression> getExpressionsWithCSE(List<? extends RowExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return ImmutableList.of();
        }
        CommonSubExpressionCollector expressionCollector = new CommonSubExpressionCollector();
        expressions.forEach(expression -> expression.accept(expressionCollector, true));
        Set<RowExpression> cse = expressionCollector.cseByLevel.values().stream().flatMap(Set::stream).collect(toImmutableSet());
        SubExpressionChecker subExpressionChecker = new SubExpressionChecker(cse);
        return expressions.stream().filter(expression -> expression.accept(subExpressionChecker, null)).collect(toImmutableList());
    }

    public static RowExpression rewriteExpressionWithCSE(RowExpression expression, Map<RowExpression, VariableReferenceExpression> rewriteWith)
    {
        ExpressionRewritter rewritter = new ExpressionRewritter(rewriteWith);
        return expression.accept(rewritter, null);
    }

    private static Map<Integer, Map<RowExpression, Integer>> removeRedundantCSE(Map<Integer, Set<RowExpression>> cseByLevel, Map<RowExpression, Integer> expressionCount)
    {
        Map<Integer, Map<RowExpression, Integer>> results = new HashMap<>();
        int startCSELevel = cseByLevel.keySet().stream().reduce(Math::max).get();
        int stopCSELevel = cseByLevel.keySet().stream().reduce(Math::min).get();
        for (int i = startCSELevel; i > stopCSELevel; i--) {
            Map<RowExpression, Integer> expressions = cseByLevel.get(i).stream().filter(expression -> expressionCount.get(expression) > 0).collect(toImmutableMap(identity(), expressionCount::get));
            if (!expressions.isEmpty()) {
                results.put(i, expressions);
            }
            for (RowExpression expression : expressions.keySet()) {
                int expressionOccurrence = expressionCount.get(expression);
                subExpressions(expression).stream()
                        .filter(subExpression -> !subExpression.equals(expression))
                        .forEach(subExpression -> {
                            if (expressionCount.containsKey(subExpression)) {
                                expressionCount.put(subExpression, expressionCount.get(subExpression) - expressionOccurrence);
                            }
                        });
            }
        }
        Map<RowExpression, Integer> expressions = cseByLevel.get(stopCSELevel).stream().filter(expression -> expressionCount.get(expression) > 0).collect(toImmutableMap(identity(), expression -> expressionCount.get(expression) + 1));
        if (!expressions.isEmpty()) {
            results.put(stopCSELevel, expressions);
        }
        return results;
    }

    static class SubExpressionChecker
            implements RowExpressionVisitor<Boolean, Void>
    {
        private final Set<RowExpression> subExpressions;

        SubExpressionChecker(Set<RowExpression> subExpressions)
        {
            this.subExpressions = subExpressions;
        }

        @Override
        public Boolean visitCall(CallExpression call, Void context)
        {
            if (subExpressions.contains(call)) {
                return true;
            }
            if (call.getArguments().isEmpty()) {
                return false;
            }
            return call.getArguments().stream().anyMatch(expression -> expression.accept(this, null));
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression reference, Void context)
        {
            return subExpressions.contains(reference);
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, Void context)
        {
            return subExpressions.contains(literal);
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return subExpressions.contains(reference);
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            if (subExpressions.contains(specialForm)) {
                return true;
            }
            if (specialForm.getArguments().isEmpty()) {
                return false;
            }
            return specialForm.getArguments().stream().anyMatch(expression -> expression.accept(this, null));
        }
    }

    static class ExpressionRewritter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Map<RowExpression, VariableReferenceExpression> expressionMap;

        public ExpressionRewritter(Map<RowExpression, VariableReferenceExpression> expressionMap)
        {
            this.expressionMap = ImmutableMap.copyOf(expressionMap);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            RowExpression rewritten = new CallExpression(
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()));
            if (expressionMap.containsKey(rewritten)) {
                return expressionMap.get(rewritten);
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
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            SpecialFormExpression rewritten = new SpecialFormExpression(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()));
            if (expressionMap.containsKey(rewritten)) {
                return expressionMap.get(rewritten);
            }
            return rewritten;
        }
    }

    static class CommonSubExpressionCollector
            implements RowExpressionVisitor<Integer, Boolean>
    {
        private static final Set<SpecialFormExpression.Form> excludedSpecialForms = ImmutableSet.of(IF, NULL_IF, SWITCH, WHEN, AND, OR);

        private final Map<Integer, Set<RowExpression>> expressionsByLevel = new HashMap<>();
        private final Map<Integer, Set<RowExpression>> cseByLevel = new HashMap<>();
        private final Map<RowExpression, Integer> expressionCount = new HashMap<>();

        private int addAtLevel(int level, RowExpression expression)
        {
            Set<RowExpression> rowExpressions = getExpresssionsAtLevel(level, expressionsByLevel);
            expressionCount.putIfAbsent(expression, 1);
            if (rowExpressions.contains(expression)) {
                getExpresssionsAtLevel(level, cseByLevel).add(expression);
                int count = expressionCount.get(expression) + 1;
                expressionCount.put(expression, count);
            }
            rowExpressions.add(expression);
            return level;
        }

        private static Set<RowExpression> getExpresssionsAtLevel(int level, Map<Integer, Set<RowExpression>> expressionsByLevel)
        {
            expressionsByLevel.putIfAbsent(level, new HashSet<>());
            return expressionsByLevel.get(level);
        }

        @Override
        public Integer visitCall(CallExpression call, Boolean collect)
        {
            if (call.getArguments().isEmpty()) {
                // Do not track leaf expression
                return 0;
            }
            int level = call.getArguments().stream().map(argument -> argument.accept(this, collect)).reduce(Math::max).get() + 1;
            if (collect) {
                return addAtLevel(level, call);
            }
            return level;
        }

        @Override
        public Integer visitInputReference(InputReferenceExpression reference, Boolean collect)
        {
            return 0;
        }

        @Override
        public Integer visitConstant(ConstantExpression literal, Boolean collect)
        {
            return 0;
        }

        @Override
        public Integer visitLambda(LambdaDefinitionExpression lambda, Boolean collect)
        {
            return 0;
        }

        @Override
        public Integer visitVariableReference(VariableReferenceExpression reference, Boolean collect)
        {
            return 0;
        }

        @Override
        public Integer visitSpecialForm(SpecialFormExpression specialForm, Boolean collect)
        {
            Boolean shouldCollect = collect && !excludedSpecialForms.contains(specialForm.getForm());
            int level = specialForm.getArguments().stream().map(argument -> argument.accept(this, shouldCollect)).reduce(Math::max).get() + 1;
            if (collect) {
                return addAtLevel(level, specialForm);
            }
            return level;
        }
    }
}
