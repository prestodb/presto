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
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.Variable;
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
import com.google.common.collect.Sets;
import com.google.common.primitives.Primitives;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.BIND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.relational.Expressions.subExpressions;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.function.Function.identity;

public class CommonSubExpressionRewriter
{
    private CommonSubExpressionRewriter() {}

    public static Map<Integer, Map<RowExpression, VariableReferenceExpression>> collectCSEByLevel(List<? extends RowExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return ImmutableMap.of();
        }

        CommonSubExpressionCollector expressionCollector = new CommonSubExpressionCollector();
        expressions.forEach(expression -> expression.accept(expressionCollector, null));
        if (expressionCollector.cseByLevel.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Integer, Map<RowExpression, Integer>> cseByLevel = removeRedundantCSE(expressionCollector.cseByLevel, expressionCollector.expressionCount);

        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        ImmutableMap.Builder<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressions = ImmutableMap.builder();
        Map<RowExpression, VariableReferenceExpression> rewriteWith = new HashMap<>();
        int startCSELevel = cseByLevel.keySet().stream().reduce(Math::min).get();
        int maxCSELevel = cseByLevel.keySet().stream().reduce(Math::max).get();
        for (int i = startCSELevel; i <= maxCSELevel; i++) {
            if (cseByLevel.containsKey(i)) {
                ExpressionRewriter rewriter = new ExpressionRewriter(rewriteWith);
                ImmutableMap.Builder<RowExpression, VariableReferenceExpression> expressionVariableMapBuilder = ImmutableMap.builder();
                for (Map.Entry<RowExpression, Integer> entry : cseByLevel.get(i).entrySet()) {
                    RowExpression rewrittenExpression = entry.getKey().accept(rewriter, null);
                    expressionVariableMapBuilder.put(rewrittenExpression, variableAllocator.newVariable(rewrittenExpression, "cse"));
                }
                Map<RowExpression, VariableReferenceExpression> expressionVariableMap = expressionVariableMapBuilder.build();
                commonSubExpressions.put(i, expressionVariableMap);
                rewriteWith.putAll(expressionVariableMap.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue())));
            }
        }
        return commonSubExpressions.build();
    }

    public static Map<Integer, Map<RowExpression, VariableReferenceExpression>> collectCSEByLevel(RowExpression expression)
    {
        return collectCSEByLevel(ImmutableList.of(expression));
    }

    public static Map<List<RowExpression>, Boolean> getExpressionsPartitionedByCSE(Collection<? extends RowExpression> expressions, int expressionGroupSize)
    {
        if (expressions.isEmpty()) {
            return ImmutableMap.of();
        }

        CommonSubExpressionCollector expressionCollector = new CommonSubExpressionCollector();
        expressions.forEach(expression -> expression.accept(expressionCollector, null));
        Set<RowExpression> cse = expressionCollector.cseByLevel.values().stream().flatMap(Set::stream).collect(toImmutableSet());

        if (cse.isEmpty()) {
            return expressions.stream().collect(toImmutableMap(ImmutableList::of, m -> false));
        }

        ImmutableMap.Builder<List<RowExpression>, Boolean> expressionsPartitionedByCse = ImmutableMap.builder();
        SubExpressionChecker subExpressionChecker = new SubExpressionChecker(cse);
        Map<Boolean, List<RowExpression>> expressionsWithCseFlag = expressions.stream().collect(Collectors.partitioningBy(expression -> expression.accept(subExpressionChecker, null)));
        expressionsWithCseFlag.get(false).forEach(expression -> expressionsPartitionedByCse.put(ImmutableList.of(expression), false));

        List<RowExpression> expressionsWithCse = expressionsWithCseFlag.get(true);
        if (expressionsWithCse.size() == 1) {
            RowExpression expression = expressionsWithCse.get(0);
            expressionsPartitionedByCse.put(ImmutableList.of(expression), true);
            return expressionsPartitionedByCse.build();
        }

        List<Set<RowExpression>> cseDependency = expressionsWithCse.stream()
                .map(expression -> subExpressions(expression).stream()
                        .filter(cse::contains)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        boolean[] merged = new boolean[expressionsWithCse.size()];

        int i = 0;
        while (i < merged.length) {
            while (i < merged.length && merged[i]) {
                i++;
            }
            if (i >= merged.length) {
                break;
            }
            merged[i] = true;
            List<RowExpression> newList = new ArrayList<>();
            newList.add(expressionsWithCse.get(i));
            Set<RowExpression> dependencies = new HashSet<>();
            Set<RowExpression> first = cseDependency.get(i);
            dependencies.addAll(first);
            int j = i + 1;
            while (j < merged.length && newList.size() < expressionGroupSize) {
                while (j < merged.length && merged[j]) {
                    j++;
                }
                if (j >= merged.length) {
                    break;
                }
                Set<RowExpression> second = cseDependency.get(j);
                if (!Sets.intersection(dependencies, second).isEmpty()) {
                    RowExpression expression = expressionsWithCse.get(j);
                    newList.add(expression);
                    dependencies.addAll(second);
                    merged[j] = true;
                    j = i + 1;
                }
                else {
                    j++;
                }
            }
            expressionsPartitionedByCse.put(ImmutableList.copyOf(newList), true);
        }

        return expressionsPartitionedByCse.build();
    }

    public static RowExpression rewriteExpressionWithCSE(RowExpression expression, Map<RowExpression, VariableReferenceExpression> rewriteWith)
    {
        ExpressionRewriter rewriter = new ExpressionRewriter(rewriteWith);
        return expression.accept(rewriter, null);
    }

    private static Map<Integer, Map<RowExpression, Integer>> removeRedundantCSE(Map<Integer, Set<RowExpression>> cseByLevel, Map<RowExpression, Integer> expressionCount)
    {
        Map<Integer, Map<RowExpression, Integer>> results = new HashMap<>();
        int startCSELevel = cseByLevel.keySet().stream().reduce(Math::max).get();
        int stopCSELevel = cseByLevel.keySet().stream().reduce(Math::min).get();
        for (int i = startCSELevel; i > stopCSELevel; i--) {
            if (!cseByLevel.containsKey(i)) {
                continue;
            }
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

    static class ExpressionRewriter
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
            implements RowExpressionVisitor<Integer, Void>
    {
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
        public Integer visitCall(CallExpression call, Void collect)
        {
            if (call.getArguments().isEmpty()) {
                // Do not track leaf expression
                return 0;
            }
            return addAtLevel(call.getArguments().stream().map(argument -> argument.accept(this, collect)).reduce(Math::max).get() + 1, call);
        }

        @Override
        public Integer visitInputReference(InputReferenceExpression reference, Void collect)
        {
            return 0;
        }

        @Override
        public Integer visitConstant(ConstantExpression literal, Void collect)
        {
            return 0;
        }

        @Override
        public Integer visitLambda(LambdaDefinitionExpression lambda, Void collect)
        {
            return 0;
        }

        @Override
        public Integer visitVariableReference(VariableReferenceExpression reference, Void collect)
        {
            return 0;
        }

        @Override
        public Integer visitSpecialForm(SpecialFormExpression specialForm, Void collect)
        {
            int level = specialForm.getArguments().stream().map(argument -> argument.accept(this, null)).reduce(Math::max).get() + 1;
            if (specialForm.getForm() != WHEN && specialForm.getForm() != BIND) {
                // BIND returns a function type rather than a value type
                // WHEN is part of CASE expression. We do not have a separate code generator to generate code for WHEN expression separately so do not consider them as CSE
                // TODO If we detect a whole WHEN statement as CSE we should probably only keep one
                addAtLevel(level, specialForm);
            }
            return level;
        }
    }

    static class CommonSubExpressionFields
    {
        private final FieldDefinition evaluatedField;
        private final FieldDefinition resultField;
        private final Class<?> resultType;
        private final String methodName;

        public CommonSubExpressionFields(FieldDefinition evaluatedField, FieldDefinition resultField, Class<?> resultType, String methodName)
        {
            this.evaluatedField = evaluatedField;
            this.resultField = resultField;
            this.resultType = resultType;
            this.methodName = methodName;
        }

        public FieldDefinition getEvaluatedField()
        {
            return evaluatedField;
        }

        public FieldDefinition getResultField()
        {
            return resultField;
        }

        public String getMethodName()
        {
            return methodName;
        }

        public Class<?> getResultType()
        {
            return resultType;
        }

        public static Map<VariableReferenceExpression, CommonSubExpressionFields> declareCommonSubExpressionFields(ClassDefinition classDefinition, Map<Integer, Map<RowExpression, VariableReferenceExpression>> commonSubExpressionsByLevel)
        {
            ImmutableMap.Builder<VariableReferenceExpression, CommonSubExpressionFields> fields = ImmutableMap.builder();
            commonSubExpressionsByLevel.values().stream().map(Map::values).flatMap(Collection::stream).forEach(variable -> {
                Class<?> type = Primitives.wrap(variable.getType().getJavaType());
                fields.put(variable, new CommonSubExpressionFields(
                        classDefinition.declareField(a(PRIVATE), variable.getName() + "Evaluated", boolean.class),
                        classDefinition.declareField(a(PRIVATE), variable.getName() + "Result", type),
                        type,
                        "get" + variable.getName()));
            });
            return fields.build();
        }

        public static void initializeCommonSubExpressionFields(Collection<CommonSubExpressionFields> cseFields, Variable thisVariable, BytecodeBlock body)
        {
            cseFields.forEach(fields -> {
                body.append(thisVariable.setField(fields.getEvaluatedField(), constantBoolean(false)));
                body.append(thisVariable.setField(fields.getResultField(), constantNull(fields.getResultType())));
            });
        }
    }
}
