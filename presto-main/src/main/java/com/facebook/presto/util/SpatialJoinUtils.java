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
package com.facebook.presto.util;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.List;

import static com.facebook.presto.metadata.BuiltInFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class SpatialJoinUtils
{
    public static final FullyQualifiedName ST_CONTAINS = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_contains");
    public static final FullyQualifiedName ST_WITHIN = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_within");
    public static final FullyQualifiedName ST_INTERSECTS = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_intersects");
    public static final FullyQualifiedName ST_DISTANCE = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_distance");
    public static final FullyQualifiedName GREAT_CIRCLE_DISTANCE = FullyQualifiedName.of(DEFAULT_NAMESPACE, "great_circle_distance");

    private SpatialJoinUtils() {}

    /**
     * Returns a subset of conjuncts matching one of the following shapes:
     * - ST_Contains(...)
     * - ST_Within(...)
     * - ST_Intersects(...)
     * <p>
     * Doesn't check or guarantee anything about function arguments.
     */
    public static List<FunctionCall> extractSupportedSpatialFunctions(Expression filterExpression)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(FunctionCall.class::isInstance)
                .map(FunctionCall.class::cast)
                .filter(SpatialJoinUtils::isSupportedSpatialFunction)
                .collect(toImmutableList());
    }

    public static List<CallExpression> extractSupportedSpatialFunctions(RowExpression filterExpression, FunctionManager functionManager)
    {
        return LogicalRowExpressions.extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> isSupportedSpatialFunction(call, functionManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialFunction(FunctionCall functionCall)
    {
        String functionName = functionCall.getName().getSuffix();
        return functionName.equalsIgnoreCase(ST_CONTAINS.getSuffix()) || functionName.equalsIgnoreCase(ST_WITHIN.getSuffix())
                || functionName.equalsIgnoreCase(ST_INTERSECTS.getSuffix());
    }

    private static boolean isSupportedSpatialFunction(CallExpression call, FunctionManager functionManager)
    {
        FullyQualifiedName functionName = functionManager.getFunctionMetadata(call.getFunctionHandle()).getName();
        return functionName.equals(ST_CONTAINS) || functionName.equals(ST_WITHIN)
                || functionName.equals(ST_INTERSECTS);
    }

    /**
     * Returns a subset of conjuncts matching one the following shapes:
     * - ST_Distance(...) <= ...
     * - ST_Distance(...) < ...
     * - ... >= ST_Distance(...)
     * - ... > ST_Distance(...)
     * <p>
     * Doesn't check or guarantee anything about ST_Distance functions arguments
     * or the other side of the comparison.
     */
    public static List<ComparisonExpression> extractSupportedSpatialComparisons(Expression filterExpression)
    {
        return extractConjuncts(filterExpression).stream()
                .filter(ComparisonExpression.class::isInstance)
                .map(ComparisonExpression.class::cast)
                .filter(SpatialJoinUtils::isSupportedSpatialComparison)
                .collect(toImmutableList());
    }

    public static List<CallExpression> extractSupportedSpatialComparisons(RowExpression filterExpression, FunctionManager functionManager)
    {
        return LogicalRowExpressions.extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> new FunctionResolution(functionManager).isComparisonFunction(call.getFunctionHandle()))
                .filter(call -> isSupportedSpatialComparison(call, functionManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialComparison(ComparisonExpression expression)
    {
        switch (expression.getOperator()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return isSTDistance(expression.getLeft()) || isGreatCircleDistance(expression.getLeft());
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return isSTDistance(expression.getRight()) || isGreatCircleDistance(expression.getRight());
            default:
                return false;
        }
    }

    private static boolean isSupportedSpatialComparison(CallExpression expression, FunctionManager functionManager)
    {
        FunctionMetadata metadata = functionManager.getFunctionMetadata(expression.getFunctionHandle());
        checkArgument(metadata.getOperatorType().isPresent() && metadata.getOperatorType().get().isComparisonOperator());
        switch (metadata.getOperatorType().get()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return isSTDistance(expression.getArguments().get(0), functionManager) || isGreatCircleDistance(expression.getArguments().get(0), functionManager);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return isSTDistance(expression.getArguments().get(1), functionManager) || isGreatCircleDistance(expression.getArguments().get(1), functionManager);
            default:
                return false;
        }
    }

    private static boolean isSTDistance(Expression expression)
    {
        if (expression instanceof FunctionCall) {
            return ((FunctionCall) expression).getName().getSuffix().equalsIgnoreCase(ST_DISTANCE.getSuffix());
        }

        return false;
    }

    private static boolean isSTDistance(RowExpression expression, FunctionManager functionManager)
    {
        if (expression instanceof CallExpression) {
            return functionManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getName().equals(ST_DISTANCE);
        }

        return false;
    }

    private static boolean isGreatCircleDistance(Expression expression)
    {
        if (expression instanceof FunctionCall) {
            return ((FunctionCall) expression).getName().getSuffix().equalsIgnoreCase(GREAT_CIRCLE_DISTANCE.getSuffix());
        }

        return false;
    }

    private static boolean isGreatCircleDistance(RowExpression expression, FunctionManager functionManager)
    {
        if (expression instanceof CallExpression) {
            return functionManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getName().equals(GREAT_CIRCLE_DISTANCE);
        }

        return false;
    }
}
