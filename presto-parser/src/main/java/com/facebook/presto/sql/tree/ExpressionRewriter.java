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
package com.facebook.presto.sql.tree;

public class ExpressionRewriter<C>
{
    public Expression rewriteExpression(Expression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public Expression rewriteRow(Row node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteArithmeticUnary(ArithmeticUnaryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteArithmeticBinary(ArithmeticBinaryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteComparisonExpression(ComparisonExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBetweenPredicate(BetweenPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNotExpression(NotExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIsNullPredicate(IsNullPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIsNotNullPredicate(IsNotNullPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNullIfExpression(NullIfExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIfExpression(IfExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSearchedCaseExpression(SearchedCaseExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSimpleCaseExpression(SimpleCaseExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteWhenClause(WhenClause node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCoalesceExpression(CoalesceExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteInListExpression(InListExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteFunctionCall(FunctionCall node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLambdaExpression(LambdaExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLikePredicate(LikePredicate node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteInPredicate(InPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSubqueryExpression(SubqueryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLiteral(Literal node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteArrayConstructor(ArrayConstructor node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSubscriptExpression(SubscriptExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteQualifiedNameReference(QualifiedNameReference node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDereferenceExpression(DereferenceExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteExtract(Extract node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCurrentTime(CurrentTime node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCast(Cast node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTryExpression(TryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteAtTimeZone(AtTimeZone node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteFieldReference(FieldReference node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }
}
