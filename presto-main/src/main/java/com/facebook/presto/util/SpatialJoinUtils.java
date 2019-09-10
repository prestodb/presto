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

import java.util.List;

import static com.facebook.presto.metadata.BuiltInFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class SpatialJoinUtils
{
    public static final FullyQualifiedName ST_CONTAINS = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_contains");
    public static final FullyQualifiedName ST_WITHIN = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_within");
    public static final FullyQualifiedName ST_INTERSECTS = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_intersects");
    public static final FullyQualifiedName ST_DISTANCE = FullyQualifiedName.of(DEFAULT_NAMESPACE, "st_distance");

    private SpatialJoinUtils() {}

    /**
     * Returns a subset of conjuncts matching one of the following shapes:
     * - ST_Contains(...)
     * - ST_Within(...)
     * - ST_Intersects(...)
     * <p>
     * Doesn't check or guarantee anything about function arguments.
     */
    public static List<CallExpression> extractSupportedSpatialFunctions(RowExpression filterExpression, FunctionManager functionManager)
    {
        return LogicalRowExpressions.extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> isSupportedSpatialFunction(call, functionManager))
                .collect(toImmutableList());
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
    public static List<CallExpression> extractSupportedSpatialComparisons(RowExpression filterExpression, FunctionManager functionManager)
    {
        return LogicalRowExpressions.extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> new FunctionResolution(functionManager).isComparisonFunction(call.getFunctionHandle()))
                .filter(call -> isSupportedSpatialComparison(call, functionManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialComparison(CallExpression expression, FunctionManager functionManager)
    {
        FunctionMetadata metadata = functionManager.getFunctionMetadata(expression.getFunctionHandle());
        checkArgument(metadata.getOperatorType().isPresent() && metadata.getOperatorType().get().isComparisonOperator());
        switch (metadata.getOperatorType().get()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return isSTDistance(expression.getArguments().get(0), functionManager);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return isSTDistance(expression.getArguments().get(1), functionManager);
            default:
                return false;
        }
    }

    private static boolean isSTDistance(RowExpression expression, FunctionManager functionManager)
    {
        if (expression instanceof CallExpression) {
            return functionManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getName().equals(ST_DISTANCE);
        }

        return false;
    }
}
