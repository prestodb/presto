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
import com.facebook.presto.common.Utils;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.relational.FunctionResolution;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isUnwrapCasts;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.isComparison;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static java.util.Objects.requireNonNull;

/**
 * Given s of type S, a constant expression t of type T, and when an implicit
 * cast exists between S->T, converts expression of the form:
 *
 * <pre>
 * CAST(s as T) = t
 * </pre>
 *
 * into
 *
 * <pre>
 * s = CAST(t as S)
 * </pre>
 *
 * For example:
 *
 * <pre>
 * CAST(x AS bigint) = bigint '1'
 *</pre>
 *
 * turns into
 *
 * <pre>
 * x = smallint '1'
 * </pre>
 *
 * It can simplify expressions that are known to be true or false, and
 * remove the comparisons altogether. For example, give x::smallint,
 * for an expression like:
 *
 * <pre>
 * CAST(x AS bigint) > bigint '10000000'
 *</pre>
 */
public class UnwrapCastInComparison
        extends RowExpressionRewriteRuleSet
{
    public UnwrapCastInComparison(Metadata metadata, ExpressionOptimizerManager expressionOptimizerManager)
    {
        super(createRewriter(metadata, expressionOptimizerManager));
    }

    private static PlanRowExpressionRewriter createRewriter(Metadata metadata, ExpressionOptimizerManager expressionOptimizerManager)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
        return (expression, context) -> UnWrapCastInComparisonRewriter.rewrite(
                expression,
                context.getSession(),
                metadata,
                expressionOptimizerManager);
    }

    public static class UnWrapCastInComparisonRewriter
    {
        private UnWrapCastInComparisonRewriter() {}

        public static RowExpression rewrite(RowExpression expression, Session session, Metadata metadata, ExpressionOptimizerManager expressionOptimizerManager)
        {
            if (isUnwrapCasts(session)) {
                RowExpression rewritten = RowExpressionTreeRewriter.rewriteWith(new Visitor(session, metadata), expression);
                return expressionOptimizerManager.getExpressionOptimizer(session.toConnectorSession()).optimize(rewritten, SERIALIZABLE, session.toConnectorSession());
            }
            return null;
        }

        private static class Visitor
                extends RowExpressionRewriter<Void>
        {
            private final Session session;
            private final FunctionAndTypeManager functionAndTypeManager;
            private final FunctionResolution functionResolution;
            private final InterpretedFunctionInvoker functionInvoker;

            public Visitor(Session session, Metadata metadata)
            {
                this.session = requireNonNull(session, "session is null");
                this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
                this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
                this.functionInvoker = new InterpretedFunctionInvoker(functionAndTypeManager);
            }

            @Override
            public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                if (!isComparison(node)) {
                    return null;
                }
                RowExpression expression = treeRewriter.defaultRewrite(node, context);
                return unwrapCast(expression);
            }

            private RowExpression unwrapCast(RowExpression rowExpression)
            {
                if (!(rowExpression instanceof CallExpression)) {
                    return null;
                }

                CallExpression callExpression = (CallExpression) rowExpression;
                if (!isComparison(callExpression) || callExpression.getArguments().size() != 2) {
                    return null;
                }

                OperatorType operatorType = functionAndTypeManager.getFunctionMetadata(callExpression.getFunctionHandle()).getOperatorType().orElse(null);
                if (operatorType == null) {
                    return null;
                }

                RowExpression leftExpression = callExpression.getArguments().get(0);
                RowExpression rightExpression = callExpression.getArguments().get(1);

                // Canonicalize Expression
                if (leftExpression instanceof ConstantExpression && !(rightExpression instanceof ConstantExpression)) {
                    leftExpression = rightExpression;
                    rightExpression = callExpression.getArguments().get(0);
                    operatorType = OperatorType.flip(operatorType);
                }

                if (!(leftExpression instanceof CallExpression)) {
                    return null;
                }

                if (!functionResolution.isCastFunction(((CallExpression) leftExpression).getFunctionHandle())) {
                    return null;
                }

                Object right = new RowExpressionInterpreter(rightExpression, functionAndTypeManager, session.toConnectorSession(), Level.OPTIMIZED).optimize();
                if (right == null || (right instanceof ConstantExpression && ((ConstantExpression) right).isNull())) {
                    switch (operatorType) {
                        case EQUAL:
                        case NOT_EQUAL:
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return constantNull(BOOLEAN);
                        case IS_DISTINCT_FROM:
                            return call("NOT", functionResolution.notFunction(), BOOLEAN, specialForm(IS_NULL, BOOLEAN, leftExpression));
                        default:
                            throw new UnsupportedOperationException("Not yet implemented");
                    }
                }

                RowExpression castArgument = ((CallExpression) leftExpression).getArguments().get(0);

                if (right instanceof RowExpression) {
                    return null;
                }

                Type sourceType = castArgument.getType();
                Type targetType = rightExpression.getType();

                if (!hasInjectiveImplicitCoercion(sourceType, targetType)) {
                    return null;
                }

                FunctionHandle sourceToTarget = functionAndTypeManager.lookupCast(CastType.CAST, sourceType, targetType);
                Optional<Type.Range> sourceRange = sourceType.getRange();
                if (sourceRange.isPresent()) {
                    Object max = sourceRange.get().getMax();
                    Object maxInTargetType = coerce(max, sourceToTarget);

                    int upperBoundComparison = compare(targetType, right, maxInTargetType);
                    if (upperBoundComparison > 0) {
                        // larger than maximum representable value
                        switch (operatorType) {
                            case EQUAL:
                            case GREATER_THAN:
                            case GREATER_THAN_OR_EQUAL:
                                return falseIfNotNull(castArgument);
                            case NOT_EQUAL:
                            case LESS_THAN:
                            case LESS_THAN_OR_EQUAL:
                                return trueIfNotNull(castArgument);
                            case IS_DISTINCT_FROM:
                                return LogicalRowExpressions.TRUE_CONSTANT;
                            default:
                                throw new UnsupportedOperationException("Not yet implemented: " + operatorType);
                        }
                    }

                    if (upperBoundComparison == 0) {
                        // equal to max representable value
                        switch (operatorType) {
                            case GREATER_THAN:
                                return falseIfNotNull(castArgument);
                            case GREATER_THAN_OR_EQUAL:
                                return comparisonExpression(functionResolution, EQUAL, castArgument, LiteralEncoder.toRowExpression(max, sourceType));
                            case LESS_THAN_OR_EQUAL:
                                return trueIfNotNull(castArgument);
                            case LESS_THAN:
                                return comparisonExpression(functionResolution, NOT_EQUAL, castArgument, LiteralEncoder.toRowExpression(max, sourceType));
                            case EQUAL:
                            case NOT_EQUAL:
                            case IS_DISTINCT_FROM:
                                return comparisonExpression(functionResolution, operatorType, castArgument, LiteralEncoder.toRowExpression(max, sourceType));
                            default:
                                throw new UnsupportedOperationException("Not yet implemented: " + operatorType);
                        }
                    }

                    Object min = sourceRange.get().getMin();
                    Object minInTargetType = coerce(min, sourceToTarget);

                    int lowerBoundComparison = compare(targetType, right, minInTargetType);
                    if (lowerBoundComparison < 0) {
                        // smaller than minimum representable value
                        switch (operatorType) {
                            case NOT_EQUAL:
                            case GREATER_THAN:
                            case GREATER_THAN_OR_EQUAL:
                                return trueIfNotNull(castArgument);
                            case EQUAL:
                            case LESS_THAN:
                            case LESS_THAN_OR_EQUAL:
                                return falseIfNotNull(castArgument);
                            case IS_DISTINCT_FROM:
                                return LogicalRowExpressions.TRUE_CONSTANT;
                            default:
                                throw new UnsupportedOperationException("Not yet implemented: " + operatorType);
                        }
                    }

                    if (lowerBoundComparison == 0) {
                        // equal to min representable value
                        switch (operatorType) {
                            case LESS_THAN:
                                return falseIfNotNull(castArgument);
                            case LESS_THAN_OR_EQUAL:
                                return comparisonExpression(functionResolution, EQUAL, castArgument, LiteralEncoder.toRowExpression(min, sourceType));
                            case GREATER_THAN_OR_EQUAL:
                                return trueIfNotNull(castArgument);
                            case GREATER_THAN:
                                return comparisonExpression(functionResolution, NOT_EQUAL, castArgument, LiteralEncoder.toRowExpression(min, sourceType));
                            case EQUAL:
                            case NOT_EQUAL:
                            case IS_DISTINCT_FROM:
                                return comparisonExpression(functionResolution, operatorType, castArgument, LiteralEncoder.toRowExpression(min, sourceType));
                            default:
                                throw new UnsupportedOperationException("Not yet implemented: " + operatorType);
                        }
                    }
                }

                FunctionHandle targetToSource;
                try {
                    targetToSource = functionAndTypeManager.lookupCast(CastType.CAST, targetType, sourceType);
                }
                catch (OperatorNotFoundException e) {
                    // Without a cast between target -> source, there's nothing more we can do
                    return null;
                }

                Object literalInSourceType;
                try {
                    literalInSourceType = coerce(right, targetToSource);
                }
                catch (PrestoException e) {
                    // A failure to cast from target -> source type could be because:
                    //  1. missing cast
                    //  2. bad implementation
                    //  3. out of range or otherwise unrepresentable value
                    // Since we can't distinguish between those cases, take the conservative option
                    // and bail out.
                    return null;
                }

                Object roundtripLiteral = coerce(literalInSourceType, sourceToTarget);

                int literalVsRoundtripped = compare(targetType, right, roundtripLiteral);

                if (literalVsRoundtripped > 0) {
                    // cast rounded down
                    switch (operatorType) {
                        case EQUAL:
                            return falseIfNotNull(castArgument);
                        case NOT_EQUAL:
                            return trueIfNotNull(castArgument);
                        case IS_DISTINCT_FROM:
                            return LogicalRowExpressions.TRUE_CONSTANT;
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            if (sourceRange.isPresent() && compare(sourceType, sourceRange.get().getMin(), literalInSourceType) == 0) {
                                return comparisonExpression(functionResolution, EQUAL, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
                            }
                            return comparisonExpression(functionResolution, LESS_THAN_OR_EQUAL, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            // We expect implicit coercions to be order-preserving, so the result of converting back from target -> source cannot produce a value
                            // larger than the next value in the source type
                            return comparisonExpression(functionResolution, GREATER_THAN, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
                        default:
                            throw new UnsupportedOperationException("Not yet implemented: " + operatorType);
                    }
                }

                if (literalVsRoundtripped < 0) {
                    // cast rounded up
                    switch (operatorType) {
                        case EQUAL:
                            return falseIfNotNull(castArgument);
                        case NOT_EQUAL:
                            return trueIfNotNull(castArgument);
                        case IS_DISTINCT_FROM:
                            return LogicalRowExpressions.TRUE_CONSTANT;
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            // We expect implicit coercions to be order-preserving, so the result of converting back from target -> source cannot produce a value
                            // smaller than the next value in the source type
                            return comparisonExpression(functionResolution, LESS_THAN, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            if (sourceRange.isPresent() && compare(sourceType, sourceRange.get().getMax(), literalInSourceType) == 0) {
                                return comparisonExpression(functionResolution, EQUAL, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
                            }
                            return comparisonExpression(functionResolution, GREATER_THAN_OR_EQUAL, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
                        default:
                            throw new UnsupportedOperationException("Not yet implemented: " + operatorType);
                    }
                }
                return comparisonExpression(functionResolution, operatorType, castArgument, LiteralEncoder.toRowExpression(literalInSourceType, sourceType));
            }

            private boolean hasInjectiveImplicitCoercion(Type source, Type target)
            {
                if ((source.equals(BIGINT) && target.equals(DOUBLE)) ||
                        (source.equals(BIGINT) && target.equals(REAL)) ||
                        (source.equals(INTEGER) && target.equals(REAL))) {
                    // Not every BIGINT fits in DOUBLE/REAL due to 64 bit vs 53-bit/23-bit mantissa. Similarly,
                    // not every INTEGER fits in a REAL (32-bit vs 23-bit mantissa)
                    return false;
                }

                if (source instanceof DecimalType) {
                    int precision = ((DecimalType) source).getPrecision();

                    if (precision > 15 && target.equals(DOUBLE)) {
                        // decimal(p,s) with p > 15 doesn't fit in a double without loss
                        return false;
                    }

                    if (precision > 7 && target.equals(REAL)) {
                        // decimal(p,s) with p > 7 doesn't fit in a double without loss
                        return false;
                    }
                }

                // Well-behaved implicit casts are injective
                return functionResolution.canCoerce(source, target);
            }

            private Object coerce(Object value, FunctionHandle coercion)
            {
                return functionInvoker.invoke(coercion, session.toConnectorSession().getSqlFunctionProperties(), value);
            }

            private int compare(Type type, Object first, Object second)
            {
                return type.compareTo(
                        Utils.nativeValueToBlock(type, first),
                        0,
                        Utils.nativeValueToBlock(type, second),
                        0);
            }

            private RowExpression falseIfNotNull(RowExpression argument)
            {
                return and(specialForm(IS_NULL, BOOLEAN, argument), constantNull(BOOLEAN));
            }

            private RowExpression trueIfNotNull(RowExpression argument)
            {
                return or(call("NOT", functionResolution.notFunction(), BOOLEAN, specialForm(IS_NULL, BOOLEAN, argument)), constantNull(BOOLEAN));
            }
        }
    }
}
