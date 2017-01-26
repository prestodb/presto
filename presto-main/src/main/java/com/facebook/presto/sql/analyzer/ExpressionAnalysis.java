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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableSet;

import java.util.IdentityHashMap;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis
{
    private final IdentityHashMap<Expression, Type> expressionTypes;
    private final IdentityHashMap<Expression, Type> expressionCoercions;
    private final Set<Expression> typeOnlyCoercions;
    private final Set<Expression> columnReferences;
    private final Set<InPredicate> subqueryInPredicates;
    private final Set<SubqueryExpression> scalarSubqueries;
    private final Set<ExistsPredicate> existsSubqueries;
    private final Set<QuantifiedComparisonExpression> quantifiedComparisons;
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final IdentityHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences;

    public ExpressionAnalysis(
            IdentityHashMap<Expression, Type> expressionTypes,
            IdentityHashMap<Expression, Type> expressionCoercions,
            Set<InPredicate> subqueryInPredicates,
            Set<SubqueryExpression> scalarSubqueries,
            Set<ExistsPredicate> existsSubqueries,
            Set<Expression> columnReferences,
            Set<Expression> typeOnlyCoercions,
            Set<QuantifiedComparisonExpression> quantifiedComparisons,
            IdentityHashMap<Identifier, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.expressionTypes = requireNonNull(expressionTypes, "expressionTypes is null");
        this.expressionCoercions = requireNonNull(expressionCoercions, "expressionCoercions is null");
        this.typeOnlyCoercions = requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null");
        this.columnReferences = ImmutableSet.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
        this.subqueryInPredicates = requireNonNull(subqueryInPredicates, "subqueryInPredicates is null");
        this.scalarSubqueries = requireNonNull(scalarSubqueries, "subqueryInPredicates is null");
        this.existsSubqueries = requireNonNull(existsSubqueries, "existsSubqueries is null");
        this.quantifiedComparisons = requireNonNull(quantifiedComparisons, "quantifiedComparisons is null");
        this.lambdaArgumentReferences = requireNonNull(lambdaArgumentReferences, "lambdaArgumentReferences is null");
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

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier qualifiedNameReference)
    {
        return lambdaArgumentReferences.get(qualifiedNameReference);
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(expression);
    }

    public Set<Expression> getColumnReferences()
    {
        return columnReferences;
    }

    public Set<InPredicate> getSubqueryInPredicates()
    {
        return subqueryInPredicates;
    }

    public Set<SubqueryExpression> getScalarSubqueries()
    {
        return scalarSubqueries;
    }

    public Set<ExistsPredicate> getExistsSubqueries()
    {
        return existsSubqueries;
    }

    public Set<QuantifiedComparisonExpression> getQuantifiedComparisons()
    {
        return quantifiedComparisons;
    }
}
