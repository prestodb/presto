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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class CommonSubExpressionRewriter
{
    private CommonSubExpressionRewriter() {}

    public static Map<Integer, Map<RowExpression, VariableReferenceExpression>> rewriteWithCSEByLevel(List<RowExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return ImmutableMap.of();
        }

        CommonSubExpressionCollector expressionCollector = new CommonSubExpressionCollector();
        expressions.forEach(expression -> expression.accept(expressionCollector, null));
        Map<Integer, Set<RowExpression>> cseByLevel = expressionCollector.cseByLevel;
        if (cseByLevel.isEmpty()) {
            return ImmutableMap.of();
        }

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

    public static Map<Integer, Map<RowExpression, VariableReferenceExpression>> rewriteWithCSEByLevel(RowExpression expression)
    {
        return rewriteWithCSEByLevel(ImmutableList.of(expression));
    }

    public static RowExpression rewriteExpressionWithCSE(RowExpression expression, Map<RowExpression, VariableReferenceExpression> rewriteWith)
    {
        ExpressionRewritter rewritter = new ExpressionRewritter(rewriteWith);
        return expression.accept(rewritter, null);
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
            implements RowExpressionVisitor<Integer, Void>
    {
        private final ExpressionEquivalence expressionEquivalence = new ExpressionEquivalence(createTestMetadataManager(), new SqlParser());
        private final Map<Integer, Set<RowExpression>> expressionsByLevel = new HashMap<>();
        private final Map<Integer, Set<RowExpression>> cseByLevel = new HashMap<>();
        private final Set<LambdaDefinitionExpression> lambdaExpressions = new HashSet<>();

        private int addAtLevel(int level, RowExpression expression)
        {
            Set<RowExpression> rowExpressions = getExpresssionsAtLevel(level, expressionsByLevel);
            if (rowExpressions.contains(expression)) {
                getExpresssionsAtLevel(level, cseByLevel).add(expression);
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
        public Integer visitCall(CallExpression call, Void context)
        {
            if (call.getArguments().isEmpty()) {
                // Do not track leaf expression
                return 0;
            }
            return addAtLevel(call.getArguments().stream().map(argument -> argument.accept(this, null)).reduce(Math::max).get() + 1, call);
        }

        @Override
        public Integer visitInputReference(InputReferenceExpression reference, Void context)
        {
            return 0;
        }

        @Override
        public Integer visitConstant(ConstantExpression literal, Void context)
        {
            return 0;
        }

        @Override
        public Integer visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            if (lambdaExpressions.stream().noneMatch(e -> expressionEquivalence.areExpressionsEquivalent(e, lambda))) {
                lambdaExpressions.add(lambda);
            }
            return 0;
        }

        @Override
        public Integer visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return 0;
        }

        @Override
        public Integer visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return addAtLevel(specialForm.getArguments().stream().map(argument -> argument.accept(this, null)).reduce(Math::max).get() + 1, specialForm);
        }
    }
}
