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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRewriteJsonExtractScalarStrposFilterEnabled;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static java.util.Objects.requireNonNull;

/**
 * Optimization rule that adds a strpos pre-filter for json_extract_scalar equality predicates.
 * <p>
 * Transforms:
 * <pre>
 *   json_extract_scalar(col, '$.key') = 'some_value'
 * </pre>
 * Into:
 * <pre>
 *   strpos(col, 'some_value') &gt; 0 AND json_extract_scalar(col, '$.key') = 'some_value'
 * </pre>
 * <p>
 * The strpos check is a cheap O(n) string scan that eliminates rows where the literal string
 * cannot possibly exist in the JSON text, avoiding expensive JSON parsing on those rows.
 * <p>
 * Safety constraints:
 * <ul>
 *   <li>Only applies to equality comparisons (=) with a string constant</li>
 *   <li>Only applies when the first argument to json_extract_scalar is VARCHAR (not JSON type)</li>
 *   <li>Only applies when the constant string contains no characters that could be
 *       JSON-escaped (", \, control chars U+0000-U+001F), which would cause the raw JSON
 *       representation to differ from the extracted scalar value</li>
 * </ul>
 */
public class RewriteJsonExtractScalarWithStrposFilter
        extends RowExpressionRewriteRuleSet
{
    public RewriteJsonExtractScalarWithStrposFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        super(new Rewriter(functionAndTypeManager));
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isRewriteJsonExtractScalarStrposFilterEnabled(session);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(filterRowExpressionRewriteRule());
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final FunctionResolution functionResolution;
        private final FunctionAndTypeManager functionAndTypeManager;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.functionAndTypeManager = functionAndTypeManager;
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            // Extract all AND conjuncts from the filter expression.
            // This avoids infinite recursion: we process top-level conjuncts and
            // check for existing strpos guards before adding new ones.
            List<RowExpression> conjuncts = extractConjuncts(expression);
            List<RowExpression> newConjuncts = new ArrayList<>();
            boolean changed = false;

            for (RowExpression conjunct : conjuncts) {
                RowExpression strposGuard = tryCreateStrposGuard(conjunct, conjuncts);
                if (strposGuard != null) {
                    newConjuncts.add(strposGuard);
                    changed = true;
                }
                newConjuncts.add(conjunct);
            }

            if (!changed) {
                return expression;
            }

            return and(newConjuncts);
        }

        /**
         * If the given conjunct matches the pattern EQUALS(json_extract_scalar(varcharCol, path), stringLiteral),
         * and no existing strpos guard is already present among the conjuncts, returns a new
         * strpos(varcharCol, literal) &gt; 0 expression. Otherwise returns null.
         */
        private RowExpression tryCreateStrposGuard(RowExpression conjunct, List<RowExpression> allConjuncts)
        {
            if (!(conjunct instanceof CallExpression)) {
                return null;
            }

            CallExpression callExpr = (CallExpression) conjunct;
            if (!functionResolution.isEqualsFunction(callExpr.getFunctionHandle()) || callExpr.getArguments().size() != 2) {
                return null;
            }

            RowExpression left = callExpr.getArguments().get(0);
            RowExpression right = callExpr.getArguments().get(1);

            // Try both operand orders: json_extract_scalar(...) = 'literal' or 'literal' = json_extract_scalar(...)
            CallExpression jsonExtractCall = null;
            ConstantExpression literalExpr = null;

            if (isJsonExtractScalarCall(left) && isStringConstant(right)) {
                jsonExtractCall = (CallExpression) left;
                literalExpr = (ConstantExpression) right;
            }
            else if (isJsonExtractScalarCall(right) && isStringConstant(left)) {
                jsonExtractCall = (CallExpression) right;
                literalExpr = (ConstantExpression) left;
            }

            if (jsonExtractCall == null || literalExpr == null) {
                return null;
            }

            RowExpression jsonInput = jsonExtractCall.getArguments().get(0);

            // Only apply when the first argument is VARCHAR (not JSON type)
            if (!(jsonInput.getType() instanceof VarcharType)) {
                return null;
            }

            // Extract the literal string value
            Slice literalSlice = (Slice) literalExpr.getValue();
            if (literalSlice == null) {
                return null;
            }
            String literalString = literalSlice.toStringUtf8();

            // Only apply when the literal is safe (no JSON-escapable characters)
            if (!isSafeForStrposOptimization(literalString)) {
                return null;
            }

            // Check if a strpos guard already exists among the conjuncts for this column and literal
            if (hasExistingStrposGuard(jsonInput, literalExpr, allConjuncts)) {
                return null;
            }

            return buildStrposGreaterThanZero(jsonInput, literalExpr);
        }

        /**
         * Checks if a strpos(jsonInput, literal) &gt; 0 conjunct already exists.
         * This prevents the rule from firing again after a previous application.
         */
        private boolean hasExistingStrposGuard(RowExpression jsonInput, ConstantExpression literal, List<RowExpression> conjuncts)
        {
            for (RowExpression conjunct : conjuncts) {
                if (isStrposGreaterThanZero(conjunct, jsonInput, literal)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Checks if the expression matches the pattern: strpos(jsonInput, literal) &gt; 0
         */
        private boolean isStrposGreaterThanZero(RowExpression expression, RowExpression expectedInput, ConstantExpression expectedLiteral)
        {
            if (!(expression instanceof CallExpression)) {
                return false;
            }

            CallExpression call = (CallExpression) expression;
            FunctionMetadata metadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());

            // Check for GREATER_THAN operator
            if (!metadata.getOperatorType().isPresent()
                    || metadata.getOperatorType().get() != GREATER_THAN
                    || call.getArguments().size() != 2) {
                return false;
            }

            RowExpression gtLeft = call.getArguments().get(0);
            RowExpression gtRight = call.getArguments().get(1);

            // Right side should be constant 0
            if (!(gtRight instanceof ConstantExpression)
                    || !BIGINT.equals(gtRight.getType())
                    || !Long.valueOf(0L).equals(((ConstantExpression) gtRight).getValue())) {
                return false;
            }

            // Left side should be strpos(expectedInput, expectedLiteral)
            if (!(gtLeft instanceof CallExpression)) {
                return false;
            }

            CallExpression strposCall = (CallExpression) gtLeft;
            FunctionMetadata strposMetadata = functionAndTypeManager.getFunctionMetadata(strposCall.getFunctionHandle());
            if (!strposMetadata.getName().getObjectName().equals("strpos") || strposCall.getArguments().size() != 2) {
                return false;
            }

            return strposCall.getArguments().get(0).equals(expectedInput)
                    && strposCall.getArguments().get(1).equals(expectedLiteral);
        }

        private boolean isJsonExtractScalarCall(RowExpression expression)
        {
            if (!(expression instanceof CallExpression)) {
                return false;
            }
            CallExpression call = (CallExpression) expression;
            FunctionMetadata metadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
            return metadata.getName().getObjectName().equals("json_extract_scalar")
                    && call.getArguments().size() == 2;
        }

        private static boolean isStringConstant(RowExpression expression)
        {
            return expression instanceof ConstantExpression
                    && expression.getType() instanceof VarcharType
                    && !((ConstantExpression) expression).isNull();
        }

        /**
         * Checks if a string literal is safe for the strpos optimization.
         * The string must not contain characters that could be escaped differently
         * in JSON representation vs. the extracted scalar value:
         * <ul>
         *   <li>Double quote (")</li>
         *   <li>Backslash (\)</li>
         *   <li>Control characters (U+0000 through U+001F)</li>
         * </ul>
         */
        private static boolean isSafeForStrposOptimization(String value)
        {
            if (value.isEmpty()) {
                return false;
            }
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '"' || c == '\\' || c <= 0x1F) {
                    return false;
                }
            }
            return true;
        }

        private RowExpression buildStrposGreaterThanZero(RowExpression jsonInput, ConstantExpression literal)
        {
            // Resolve strpos(varchar, varchar) -> bigint
            FunctionHandle strposHandle = functionAndTypeManager.lookupFunction(
                    "strpos",
                    fromTypes(jsonInput.getType(), literal.getType()));

            CallExpression strposCall = call(
                    "strpos",
                    strposHandle,
                    BIGINT,
                    ImmutableList.of(jsonInput, literal));

            // Resolve GREATER_THAN(bigint, bigint)
            FunctionHandle greaterThanHandle = functionAndTypeManager.resolveOperator(
                    GREATER_THAN,
                    fromTypes(BIGINT, BIGINT));

            return call(
                    GREATER_THAN.getOperator(),
                    greaterThanHandle,
                    BOOLEAN,
                    ImmutableList.of(strposCall, constant(0L, BIGINT)));
        }
    }
}
