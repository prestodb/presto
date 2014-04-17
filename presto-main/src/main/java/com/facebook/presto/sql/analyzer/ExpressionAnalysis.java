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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.spi.type.Type;

import java.util.IdentityHashMap;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExpressionAnalysis
{
    private final IdentityHashMap<Expression, Type> expressionTypes;
    private final IdentityHashMap<Expression, Type> expressionCoercions;
    private final IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions;
    private final Set<InPredicate> subqueryInPredicates;

    public ExpressionAnalysis(
            IdentityHashMap<Expression, Type> expressionTypes,
            IdentityHashMap<Expression, Type> expressionCoercions,
            IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions,
            Set<InPredicate> subqueryInPredicates)
    {
        this.expressionTypes = checkNotNull(expressionTypes, "expressionTypes is null");
        this.expressionCoercions = checkNotNull(expressionCoercions, "expressionCoercions is null");
        this.resolvedFunctions = checkNotNull(resolvedFunctions, "resolvedFunctions is null");
        this.subqueryInPredicates = checkNotNull(subqueryInPredicates, "subqueryInPredicates is null");
    }

    public Type getType(Expression expression)
    {
        return expressionTypes.get(expression);
    }

    public IdentityHashMap<Expression, Type> getExpressionTypes()
    {
        return expressionTypes;
    }

    public Type getCoercion(Expression expression)
    {
        return expressionCoercions.get(expression);
    }

    public IdentityHashMap<Expression, Type> getExpressionCoercions()
    {
        return expressionCoercions;
    }

    public IdentityHashMap<FunctionCall, FunctionInfo> getResolvedFunctions()
    {
        return resolvedFunctions;
    }

    public Set<InPredicate> getSubqueryInPredicates()
    {
        return subqueryInPredicates;
    }
}
