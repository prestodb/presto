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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.Utils;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.optimizations.RowExpressionCanonicalizer;

import java.util.Optional;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.sql.relational.DomainExtractor.PredicateOptimizer;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.TRUE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.and;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.or;
import static java.util.Objects.requireNonNull;

public class InterpretedPredicateOptimizer
        implements PredicateOptimizer
{
    private final Metadata metadata;
    private final Session session;
    private final FunctionManager functionManager;
    private final InterpretedFunctionInvoker functionInvoker;
    private final RowExpressionCanonicalizer canonicalizer;

    public InterpretedPredicateOptimizer(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
        this.functionManager = metadata.getFunctionManager();
        this.functionInvoker = new InterpretedFunctionInvoker(functionManager);
        this.canonicalizer = new RowExpressionCanonicalizer(functionManager);
    }

    @Override
    public RowExpression optimize(RowExpression input)
    {
        if (input instanceof ConstantExpression || input instanceof VariableReferenceExpression || input instanceof InputReferenceExpression) {
            return input;
        }
        Object result = new RowExpressionInterpreter(input, metadata, session, true).optimize();
        if (result instanceof RowExpression) {
            return canonicalizer.canonicalize((RowExpression) result);
        }
        return toRowExpression(result, input.getType());
    }

    @Override
    public Optional<RowExpression> coerceComparisonWithRounding(
            Type expressionType,
            RowExpression compareTarget,
            ConstantExpression constant,
            OperatorType comparisonOperator)
    {
        requireNonNull(constant, "nullableValue is null");
        if (constant.getValue() == null) {
            return Optional.empty();
        }
        Type valueType = constant.getType();
        Object value = constant.getValue();
        return floorValue(valueType, expressionType, value)
                .map((floorValue) -> rewriteComparisonExpression(expressionType, compareTarget, valueType, value, floorValue, comparisonOperator));
    }

    private RowExpression rewriteComparisonExpression(
            Type expressionType,
            RowExpression compareTarget,
            Type valueType,
            Object originalValue,
            Object coercedValue,
            OperatorType comparisonOperator)
    {
        int originalComparedToCoerced = compareOriginalValueToCoerced(valueType, originalValue, expressionType, coercedValue);
        boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
        boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;
        boolean coercedValueIsGreaterThanOriginal = originalComparedToCoerced < 0;
        RowExpression coercedLiteral = toRowExpression(coercedValue, expressionType);

        switch (comparisonOperator) {
            case GREATER_THAN_OR_EQUAL:
            case GREATER_THAN: {
                if (coercedValueIsGreaterThanOriginal) {
                    return binaryOperator(GREATER_THAN_OR_EQUAL, compareTarget, coercedLiteral);
                }
                if (coercedValueIsEqualToOriginal) {
                    return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                }
                return binaryOperator(GREATER_THAN, compareTarget, coercedLiteral);
            }
            case LESS_THAN_OR_EQUAL:
            case LESS_THAN: {
                if (coercedValueIsLessThanOriginal) {
                    return binaryOperator(LESS_THAN_OR_EQUAL, compareTarget, coercedLiteral);
                }
                if (coercedValueIsEqualToOriginal) {
                    return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                }
                return binaryOperator(LESS_THAN, compareTarget, coercedLiteral);
            }
            case EQUAL: {
                if (coercedValueIsEqualToOriginal) {
                    return binaryOperator(EQUAL, compareTarget, coercedLiteral);
                }
                // Return something that is false for all non-null values
                return and(binaryOperator(EQUAL, compareTarget, coercedLiteral),
                        binaryOperator(NOT_EQUAL, compareTarget, coercedLiteral));
            }
            case NOT_EQUAL: {
                if (coercedValueIsEqualToOriginal) {
                    return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                }
                // Return something that is true for all non-null values
                return or(binaryOperator(EQUAL, compareTarget, coercedLiteral),
                        binaryOperator(NOT_EQUAL, compareTarget, coercedLiteral));
            }
            case IS_DISTINCT_FROM: {
                if (coercedValueIsEqualToOriginal) {
                    return binaryOperator(comparisonOperator, compareTarget, coercedLiteral);
                }
                return TRUE;
            }
        }

        throw new IllegalArgumentException("Unhandled operator: " + comparisonOperator);
    }

    private RowExpression binaryOperator(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        return call(
                operatorType.name(),
                metadata.getFunctionManager().resolveOperator(operatorType, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }

    private int compareOriginalValueToCoerced(Type originalValueType, Object originalValue, Type coercedValueType, Object coercedValue)
    {
        FunctionHandle castToOriginalTypeOperator = functionManager.lookupCast(CAST, coercedValueType.getTypeSignature(), originalValueType.getTypeSignature());
        Object coercedValueInOriginalType = functionInvoker.invoke(castToOriginalTypeOperator, session.toConnectorSession(), coercedValue);
        Block originalValueBlock = Utils.nativeValueToBlock(originalValueType, originalValue);
        Block coercedValueBlock = Utils.nativeValueToBlock(originalValueType, coercedValueInOriginalType);
        return originalValueType.compareTo(originalValueBlock, 0, coercedValueBlock, 0);
    }

    private Optional<Object> floorValue(Type fromType, Type toType, Object value)
    {
        return getSaturatedFloorCastOperator(fromType, toType)
                .map((operator) -> functionInvoker.invoke(operator, session.toConnectorSession(), value));
    }

    private Optional<FunctionHandle> getSaturatedFloorCastOperator(Type fromType, Type toType)
    {
        try {
            return Optional.of(functionManager.lookupCast(SATURATED_FLOOR_CAST, fromType.getTypeSignature(), toType.getTypeSignature()));
        }
        catch (OperatorNotFoundException e) {
            return Optional.empty();
        }
    }
}
