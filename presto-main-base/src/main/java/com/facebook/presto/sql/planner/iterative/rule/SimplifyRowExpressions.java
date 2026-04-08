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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SimplifyRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public SimplifyRowExpressions(Metadata metadata, ExpressionOptimizerManager expressionOptimizerManager)
    {
        super(new Rewriter(metadata, expressionOptimizerManager));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final NestedIfSimplifier nestedIfSimplifier;
        private final MapFromEntriesRewriter mapFromEntriesRewriter;
        private final ExpressionOptimizerManager expressionOptimizerManager;
        private final LogicalExpressionRewriter logicalExpressionRewriter;

        public Rewriter(Metadata metadata, ExpressionOptimizerManager expressionOptimizerManager)
        {
            requireNonNull(metadata, "metadata is null");
            requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
            this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
            this.logicalExpressionRewriter = new LogicalExpressionRewriter(metadata.getFunctionAndTypeManager());
            this.nestedIfSimplifier = new NestedIfSimplifier(new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()));
            this.mapFromEntriesRewriter = new MapFromEntriesRewriter(metadata.getFunctionAndTypeManager());
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return rewrite(expression, context.getSession());
        }

        private RowExpression rewrite(RowExpression expression, Session session)
        {
            // Rewrite RowExpression first to reduce depth of RowExpression tree by balancing AND/OR predicates.
            // It doesn't matter whether we rewrite/optimize first because this will be called by IterativeOptimizer.
            RowExpression rewritten = RowExpressionTreeRewriter.rewriteWith(logicalExpressionRewriter, expression, true);
            rewritten = RowExpressionTreeRewriter.rewriteWith(nestedIfSimplifier, rewritten);
            rewritten = RowExpressionTreeRewriter.rewriteWith(mapFromEntriesRewriter, rewritten);
            return expressionOptimizerManager.getExpressionOptimizer(session.toConnectorSession()).optimize(rewritten, SERIALIZABLE, session.toConnectorSession());
        }
    }

    @VisibleForTesting
    public static RowExpression rewrite(RowExpression expression, Metadata metadata, Session session, ExpressionOptimizerManager expressionOptimizerManager)
    {
        return new Rewriter(metadata, expressionOptimizerManager).rewrite(expression, session);
    }

    /**
     * Simplifies nested IF expressions where the outer and inner false
     * branches are identical:
     * <pre>
     *   IF(x, IF(y, v, E), E) &rarr; IF(x AND y, v, E)
     * </pre>
     * This covers the common case where E is null (explicit or omitted ELSE),
     * as well as any other matching expression.
     * <p>
     * The rewrite only applies when the inner condition {@code y} is
     * deterministic, because the original form does not evaluate {@code y}
     * when {@code x} is NULL or FALSE, whereas {@code x AND y} may evaluate
     * {@code y} in those cases.
     * <p>
     * Uses bottom-up rewriting so that deeply nested IFs are fully
     * flattened in a single pass.
     */
    private static class NestedIfSimplifier
            extends RowExpressionRewriter<Void>
    {
        private final RowExpressionDeterminismEvaluator determinismEvaluator;

        NestedIfSimplifier(RowExpressionDeterminismEvaluator determinismEvaluator)
        {
            this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getForm() != IF) {
                return null;
            }

            // Recursively simplify children first (bottom-up)
            SpecialFormExpression rewritten = treeRewriter.defaultRewrite(node, context);

            List<RowExpression> args = rewritten.getArguments();
            RowExpression condition = args.get(0);
            RowExpression trueValue = args.get(1);
            RowExpression falseValue = args.get(2);

            if (trueValue instanceof SpecialFormExpression
                    && ((SpecialFormExpression) trueValue).getForm() == IF) {
                SpecialFormExpression innerIf = (SpecialFormExpression) trueValue;
                List<RowExpression> innerArgs = innerIf.getArguments();
                RowExpression innerCondition = innerArgs.get(0);
                if (falseValue.equals(innerArgs.get(2)) && determinismEvaluator.isDeterministic(innerCondition)) {
                    RowExpression combinedCondition = new SpecialFormExpression(rewritten.getSourceLocation(), AND, BOOLEAN, condition, innerCondition);
                    return new SpecialFormExpression(rewritten.getSourceLocation(), IF, rewritten.getType(), combinedCondition, innerArgs.get(1), falseValue);
                }
            }

            return rewritten == node ? null : rewritten;
        }
    }

    /**
     * Rewrites {@code map_from_entries(ARRAY[ROW(k1,v1), ROW(k2,v2), ...])}
     * into {@code MAP(ARRAY[k1,k2,...], ARRAY[v1,v2,...])}, eliminating the
     * overhead of constructing and then immediately destructuring ROW objects.
     * <p>
     * Only fires when the argument is a literal array constructor whose every
     * element is a two-field {@code ROW_CONSTRUCTOR}.
     */
    private static class MapFromEntriesRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionResolution functionResolution;

        MapFromEntriesRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getArguments().size() != 1 ||
                    !functionAndTypeManager.getFunctionMetadata(node.getFunctionHandle()).getName().getObjectName().equals("map_from_entries")) {
                return null;
            }

            RowExpression argument = node.getArguments().get(0);
            if (!(argument instanceof CallExpression)) {
                return null;
            }

            CallExpression arrayCall = (CallExpression) argument;
            if (!functionResolution.isArrayConstructor(arrayCall.getFunctionHandle())) {
                return null;
            }

            List<RowExpression> entries = arrayCall.getArguments();
            if (entries.isEmpty()) {
                return null;
            }

            List<RowExpression> keys = new ArrayList<>();
            List<RowExpression> values = new ArrayList<>();
            Set<RowExpression> seenKeys = new HashSet<>();
            for (RowExpression entry : entries) {
                if (!(entry instanceof SpecialFormExpression)
                        || ((SpecialFormExpression) entry).getForm() != ROW_CONSTRUCTOR) {
                    return null;
                }
                SpecialFormExpression row = (SpecialFormExpression) entry;
                if (row.getArguments().size() != 2) {
                    return null;
                }
                RowExpression key = row.getArguments().get(0);
                if (!seenKeys.add(key)) {
                    // Duplicate key detected; skip rewrite to preserve
                    // the original map_from_entries error message
                    return null;
                }
                keys.add(key);
                values.add(row.getArguments().get(1));
            }

            // Derive key/value types from the map type to handle coerced types correctly
            MapType mapType = (MapType) node.getType();
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();

            // Skip rewrite for ROW key types: map_from_entries and MAP() throw
            // different error codes for null-containing ROW keys, so rewriting
            // would change observable error semantics
            if (keyType instanceof RowType) {
                return null;
            }

            RowExpression keyArray = buildArrayConstructor(keyType, keys);
            RowExpression valueArray = buildArrayConstructor(valueType, values);

            return call(functionAndTypeManager, "MAP", node.getType(), keyArray, valueArray);
        }

        private RowExpression buildArrayConstructor(Type elementType, List<RowExpression> elements)
        {
            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (RowExpression element : elements) {
                types.add(element.getType());
            }
            return call("ARRAY",
                    functionResolution.arrayConstructor(types.build()),
                    new ArrayType(elementType),
                    elements);
        }
    }

    private static class LogicalExpressionRewriter
            extends RowExpressionRewriter<Boolean>
    {
        private final FunctionResolution functionResolution;
        private final LogicalRowExpressions logicalRowExpressions;

        public LogicalExpressionRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionManager is null");
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionAndTypeManager), new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()), functionAndTypeManager);
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (functionResolution.isNotFunction(node.getFunctionHandle())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "NOT must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (isConjunctiveDisjunctive(node.getForm())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "AND/OR must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        private boolean isConjunctiveDisjunctive(Form form)
        {
            return form == AND || form == OR;
        }

        private RowExpression rewriteBooleanExpression(RowExpression expression, boolean isRoot)
        {
            if (isRoot) {
                return logicalRowExpressions.convertToConjunctiveNormalForm(expression);
            }
            return logicalRowExpressions.minimalNormalForm(expression);
        }
    }
}
