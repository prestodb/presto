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
import java.util.Set;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.NULL_IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.relational.Expressions.subExpressions;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class CommonSubExpressionRewriter
{
    private CommonSubExpressionRewriter() {}

    public static Map<Integer, Map<RowExpression, VariableReferenceExpression>> collectCSEByLevel(List<? extends RowExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return ImmutableMap.of();
        }

        CommonSubExpressionCollector expressionCollector = new CommonSubExpressionCollector();
        expressions.forEach(expression -> expression.accept(expressionCollector, true));
        Map<Integer, Set<RowExpression>> cseByLevel = expressionCollector.cseByLevel;
        if (cseByLevel.isEmpty()) {
            return ImmutableMap.of();
        }

        cseByLevel  = removeRedundantCSE(cseByLevel, expressionCollector.expressionCount);

        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        ImmutableMap.Builder<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressions = ImmutableMap.builder();
        Map<RowExpression, VariableReferenceExpression> rewriteWith = new HashMap<>();
        int startCSELevel = cseByLevel.keySet().stream().reduce(Math::min).get();
        int maxCSELevel = cseByLevel.keySet().stream().reduce(Math::max).get();
        for (int i = startCSELevel; i <= maxCSELevel; i++) {
            if (cseByLevel.containsKey(i)) {
                ExpressionRewritter rewritter = new ExpressionRewritter(rewriteWith);
                ImmutableMap.Builder<RowExpression, VariableReferenceExpression> expressionVariableMapBuilder = ImmutableMap.builder();
                for (RowExpression expression : cseByLevel.get(i)) {
                    RowExpression rewritten = expression.accept(rewritter, null);
                    expressionVariableMapBuilder.put(rewritten, variableAllocator.newVariable(rewritten, "cse"));
                }
                Map<RowExpression, VariableReferenceExpression> expressionVariableMap = expressionVariableMapBuilder.build();
                commonSubExpressions.put(i, expressionVariableMap);
                rewriteWith.putAll(expressionVariableMap);
            }
        }
        return commonSubExpressions.build();
    }

    public static Map<Integer, Map<RowExpression, VariableReferenceExpression>> collectCSEByLevel(RowExpression expression)
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

    private static Map<Integer, Set<RowExpression>> removeRedundantCSE(Map<Integer, Set<RowExpression>> cseByLevel, Map<RowExpression, Integer> expressionCount)
    {
        Map<Integer, Set<RowExpression>> results = new HashMap<>();
        int startCSELevel = cseByLevel.keySet().stream().reduce(Math::max).get();
        int stopCSELevel = cseByLevel.keySet().stream().reduce(Math::min).get();
        for (int i = startCSELevel; i > stopCSELevel; i--) {
            Set<RowExpression> expressions = cseByLevel.get(i).stream().filter(expression -> expressionCount.get(expression) > 0).collect(toImmutableSet());
            if (!expressions.isEmpty()) {
                results.put(i, expressions);
            }
            for (RowExpression expression : expressions) {
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
        Set<RowExpression> expressions = cseByLevel.get(stopCSELevel).stream().filter(expression -> expressionCount.get(expression) > 0).collect(toImmutableSet());
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
