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
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.SubqueryExpression;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
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

    public ExpressionAnalysis(
            Map<NodeRef<Expression>, Type> expressionTypes,
            Map<NodeRef<Expression>, Type> expressionCoercions,
            Set<NodeRef<InPredicate>> subqueryInPredicates,
            Set<NodeRef<SubqueryExpression>> scalarSubqueries,
            Set<NodeRef<ExistsPredicate>> existsSubqueries,
            Map<NodeRef<Expression>, FieldId> columnReferences,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons,
            Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.expressionTypes = new LinkedHashMap<>(requireNonNull(expressionTypes, "expressionTypes is null"));
        this.expressionCoercions = new LinkedHashMap<>(requireNonNull(expressionCoercions, "expressionCoercions is null"));
        this.typeOnlyCoercions = new LinkedHashSet<>(requireNonNull(typeOnlyCoercions, "typeOnlyCoercions is null"));
        this.columnReferences = new LinkedHashMap<>(requireNonNull(columnReferences, "columnReferences is null"));
        this.subqueryInPredicates = new LinkedHashSet<>(requireNonNull(subqueryInPredicates, "subqueryInPredicates is null"));
        this.scalarSubqueries = new LinkedHashSet<>(requireNonNull(scalarSubqueries, "subqueryInPredicates is null"));
        this.existsSubqueries = new LinkedHashSet<>(requireNonNull(existsSubqueries, "existsSubqueries is null"));
        this.quantifiedComparisons = new LinkedHashSet<>(requireNonNull(quantifiedComparisons, "quantifiedComparisons is null"));
        this.lambdaArgumentReferences = new LinkedHashMap<>(requireNonNull(lambdaArgumentReferences, "lambdaArgumentReferences is null"));
    }

    public Type getType(Expression expression)
    {
        return expressionTypes.get(NodeRef.of(expression));
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return unmodifiableMap(expressionTypes);
    }

    public Type getCoercion(Expression expression)
    {
        return expressionCoercions.get(NodeRef.of(expression));
    }

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier qualifiedNameReference)
    {
        return lambdaArgumentReferences.get(NodeRef.of(qualifiedNameReference));
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
        return unmodifiableSet(subqueryInPredicates);
    }

    public Set<NodeRef<SubqueryExpression>> getScalarSubqueries()
    {
        return unmodifiableSet(scalarSubqueries);
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries()
    {
        return unmodifiableSet(existsSubqueries);
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return unmodifiableSet(quantifiedComparisons);
    }
}
