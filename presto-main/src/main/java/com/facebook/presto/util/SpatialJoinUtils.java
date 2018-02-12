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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class SpatialJoinUtils
{
    public static final String ST_CONTAINS = "st_contains";
    public static final String ST_INTERSECTS = "st_intersects";

    private SpatialJoinUtils() {}

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

    public static boolean isSpatialJoinFilter(PlanNode left, PlanNode right, Expression filterExpression)
    {
        List<FunctionCall> functionCalls = extractSupportedSpatialFunctions(filterExpression);
        for (FunctionCall functionCall : functionCalls) {
            List<Expression> arguments = functionCall.getArguments();
            verify(arguments.size() == 2);
            if (!(arguments.get(0) instanceof SymbolReference) || !(arguments.get(1) instanceof SymbolReference)) {
                continue;
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
        }

        return false;
    }

    private static Set<SymbolReference> getSymbolReferences(Collection<Symbol> symbols)
    {
        return symbols.stream().map(Symbol::toSymbolReference).collect(toImmutableSet());
    }
}
