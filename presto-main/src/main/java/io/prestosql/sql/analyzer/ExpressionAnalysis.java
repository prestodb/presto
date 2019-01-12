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
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.SubqueryExpression;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis
{
    private final Map<NodeRef<Expression>, Type> expressionTypes;
    private final Map<NodeRef<Expression>, Type> expressionCoercions;
    private final Set<NodeRef<Expression>> typeOnlyCoercions;
    private final Map<NodeRef<Expression>, FieldId> columnReferences;
    private final Set<NodeRef<InPredicate>> subqueryInPredicates;
    private final Set<NodeRef<SubqueryExpression>> scalarSubqueries;
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries;
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons;
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences;
    private final Set<NodeRef<FunctionCall>> windowFunctions;

    public ExpressionAnalysis(
            Map<NodeRef<Expression>, Type> expressionTypes,
            Map<NodeRef<Expression>, Type> expressionCoercions,
            Set<NodeRef<InPredicate>> subqueryInPredicates,
            Set<NodeRef<SubqueryExpression>> scalarSubqueries,
            Set<NodeRef<ExistsPredicate>> existsSubqueries,
            Map<NodeRef<Expression>, FieldId> columnReferences,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons,
            Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences,
            Set<NodeRef<FunctionCall>> windowFunctions)
    {
        this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
        this.expressionCoercions = ImmutableMap.copyOf(requireNonNull(expressionCoercions, "expressionCoercions is null"));
        this.typeOnlyCoercions = ImmutableSet.copyOf(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
        this.columnReferences = ImmutableMap.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
        this.subqueryInPredicates = ImmutableSet.copyOf(requireNonNull(subqueryInPredicates, "subqueryInPredicates is null"));
        this.scalarSubqueries = ImmutableSet.copyOf(requireNonNull(scalarSubqueries, "subqueryInPredicates is null"));
        this.existsSubqueries = ImmutableSet.copyOf(requireNonNull(existsSubqueries, "existsSubqueries is null"));
        this.quantifiedComparisons = ImmutableSet.copyOf(requireNonNull(quantifiedComparisons, "quantifiedComparisons is null"));
        this.lambdaArgumentReferences = ImmutableMap.copyOf(requireNonNull(lambdaArgumentReferences, "lambdaArgumentReferences is null"));
        this.windowFunctions = ImmutableSet.copyOf(requireNonNull(windowFunctions, "windowFunctions is null"));
    }

    public Type getType(Expression expression)
    {
        return expressionTypes.get(NodeRef.of(expression));
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return expressionTypes;
    }

    public Type getCoercion(Expression expression)
    {
        return expressionCoercions.get(NodeRef.of(expression));
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(NodeRef.of(expression));
    }

    public boolean isColumnReference(Expression node)
    {
        return columnReferences.containsKey(NodeRef.of(node));
    }

    public Set<NodeRef<InPredicate>> getSubqueryInPredicates()
    {
        return subqueryInPredicates;
    }

    public Set<NodeRef<SubqueryExpression>> getScalarSubqueries()
    {
        return scalarSubqueries;
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries()
    {
        return existsSubqueries;
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return quantifiedComparisons;
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions()
    {
        return windowFunctions;
    }
}
