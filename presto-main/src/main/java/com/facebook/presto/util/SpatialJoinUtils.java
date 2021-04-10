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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

public class SpatialJoinUtils
{
    public static final QualifiedObjectName ST_CONTAINS = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_contains");
    public static final QualifiedObjectName ST_CROSSES = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_crosses");
    public static final QualifiedObjectName ST_EQUALS = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_equals");
    public static final QualifiedObjectName ST_INTERSECTS = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_intersects");
    public static final QualifiedObjectName ST_OVERLAPS = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_overlaps");
    public static final QualifiedObjectName ST_TOUCHES = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_touches");
    public static final QualifiedObjectName ST_WITHIN = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_within");
    public static final QualifiedObjectName ST_DISTANCE = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "st_distance");

    private static final Set<String> ALLOWED_SPATIAL_JOIN_FUNCTIONS = Stream.of(
            ST_CONTAINS, ST_CROSSES, ST_EQUALS, ST_INTERSECTS, ST_OVERLAPS, ST_TOUCHES, ST_WITHIN)
            .map(QualifiedObjectName::getObjectName)
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    private SpatialJoinUtils() {}

    /**
     * Returns a subset of conjuncts matching one of the following shapes:
     * - ST_Contains(...)
     * - ST_Within(...)
     * - ST_Intersects(...)
     * <p>
     * Doesn't check or guarantee anything about function arguments.
     */
    public static List<CallExpression> extractSupportedSpatialFunctions(RowExpression filterExpression, FunctionAndTypeManager functionAndTypeManager)
    {
        return LogicalRowExpressions.extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> isSupportedSpatialFunction(call, functionAndTypeManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialFunction(CallExpression call, FunctionAndTypeManager functionAndTypeManager)
    {
        String functionName = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getName().getObjectName().toLowerCase(ENGLISH);
        return ALLOWED_SPATIAL_JOIN_FUNCTIONS.contains(functionName);
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
    public static List<CallExpression> extractSupportedSpatialComparisons(RowExpression filterExpression, FunctionAndTypeManager functionAndTypeManager)
    {
        return LogicalRowExpressions.extractConjuncts(filterExpression).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(call -> new FunctionResolution(functionAndTypeManager).isComparisonFunction(call.getFunctionHandle()))
                .filter(call -> isSupportedSpatialComparison(call, functionAndTypeManager))
                .collect(toImmutableList());
    }

    private static boolean isSupportedSpatialComparison(CallExpression expression, FunctionAndTypeManager functionAndTypeManager)
    {
        FunctionMetadata metadata = functionAndTypeManager.getFunctionMetadata(expression.getFunctionHandle());
        checkArgument(metadata.getOperatorType().isPresent() && metadata.getOperatorType().get().isComparisonOperator());
        switch (metadata.getOperatorType().get()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return isSTDistance(expression.getArguments().get(0), functionAndTypeManager);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return isSTDistance(expression.getArguments().get(1), functionAndTypeManager);
            default:
                return false;
        }
    }

    private static boolean isSTDistance(RowExpression expression, FunctionAndTypeManager functionAndTypeManager)
    {
        return expression instanceof CallExpression && functionAndTypeManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getName().equals(ST_DISTANCE);
    }

    public static FunctionHandle getFlippedFunctionHandle(CallExpression callExpression, FunctionAndTypeManager functionAndTypeManager)
    {
        FunctionMetadata callExpressionMetadata = functionAndTypeManager.getFunctionMetadata(callExpression.getFunctionHandle());
        checkArgument(callExpressionMetadata.getOperatorType().isPresent());
        OperatorType operatorType = flip(callExpressionMetadata.getOperatorType().get());
        List<TypeSignatureProvider> typeProviderList = fromTypes(callExpression.getArguments().stream().map(RowExpression::getType).collect(toImmutableList()));
        checkArgument(typeProviderList.size() == 2, "Expected there to be only two arguments in type provider");
        return functionAndTypeManager.resolveOperator(
                operatorType,
                ImmutableList.of(typeProviderList.get(1), typeProviderList.get(0)));
    }

    public static OperatorType flip(OperatorType operatorType)
    {
        switch (operatorType) {
            case EQUAL:
                return EQUAL;
            case NOT_EQUAL:
                return NOT_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
        }
    }
}
