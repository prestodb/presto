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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class SpatialJoinUtils
{
    public static final String ST_CONTAINS = "st_contains";
    public static final String ST_INTERSECTS = "st_intersects";
    public static final String ST_DISTANCE = "st_distance";

    private SpatialJoinUtils() {}

    /**
     * Returns a subset of conjuncts matching one of the following shapes:
     * - ST_Contains(...)
     * - ST_Intersects(...)
     *
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

    private static boolean isSupportedSpatialFunction(FunctionCall functionCall)
    {
        String functionName = functionCall.getName().toString();
        return functionName.equalsIgnoreCase(ST_CONTAINS) || functionName.equalsIgnoreCase(ST_INTERSECTS);
    }

    /**
     * Returns a subset of conjuncts matching one the following shapes:
     * - ST_Distance(...) <= ...
     * - ST_Distance(...) < ...
     * - ... >= ST_Distance(...)
     * - ... > ST_Distance(...)
     *
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

    private static boolean isSupportedSpatialComparison(ComparisonExpression expression)
    {
        switch (expression.getOperator()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return isSTDistance(expression.getLeft());
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return isSTDistance(expression.getRight());
            default:
                return false;
        }
    }

    private static boolean isSTDistance(Expression expression)
    {
        if (expression instanceof FunctionCall) {
            return ((FunctionCall) expression).getName().toString().equalsIgnoreCase(ST_DISTANCE);
        }

        return false;
    }

    public static boolean isSpatialJoinFilter(PlanNode left, PlanNode right, Expression filterExpression)
    {
        List<FunctionCall> functionCalls = extractSupportedSpatialFunctions(filterExpression);
        for (FunctionCall functionCall : functionCalls) {
            if (isSpatialJoinFilter(left, right, functionCall)) {
                return true;
            }
        }

        List<ComparisonExpression> spatialComparisons = extractSupportedSpatialComparisons(filterExpression);
        for (ComparisonExpression spatialComparison : spatialComparisons) {
            if (spatialComparison.getOperator() == LESS_THAN || spatialComparison.getOperator() == LESS_THAN_OR_EQUAL) {
                // ST_Distance(a, b) <= r
                Expression radius = spatialComparison.getRight();
                if (radius instanceof Literal || (radius instanceof SymbolReference && getSymbolReferences(right.getOutputSymbols()).contains(radius))) {
                    if (isSpatialJoinFilter(left, right, (FunctionCall) spatialComparison.getLeft())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static boolean isSpatialJoinFilter(PlanNode left, PlanNode right, FunctionCall spatialFunction)
    {
        List<Expression> arguments = spatialFunction.getArguments();
        verify(arguments.size() == 2);
        if (!(arguments.get(0) instanceof SymbolReference) || !(arguments.get(1) instanceof SymbolReference)) {
            return false;
        }

        SymbolReference firstSymbol = (SymbolReference) arguments.get(0);
        SymbolReference secondSymbol = (SymbolReference) arguments.get(1);

        Set<SymbolReference> probeSymbols = getSymbolReferences(left.getOutputSymbols());
        Set<SymbolReference> buildSymbols = getSymbolReferences(right.getOutputSymbols());

        if (probeSymbols.contains(firstSymbol) && buildSymbols.contains(secondSymbol)) {
            return true;
        }

        if (probeSymbols.contains(secondSymbol) && buildSymbols.contains(firstSymbol)) {
            return true;
        }

        return false;
    }

    private static Set<SymbolReference> getSymbolReferences(Collection<Symbol> symbols)
    {
        return symbols.stream().map(Symbol::toSymbolReference).collect(toImmutableSet());
    }
}
