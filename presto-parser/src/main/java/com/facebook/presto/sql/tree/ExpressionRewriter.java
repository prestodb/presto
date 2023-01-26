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

    public Expression rewriteBindExpression(BindExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
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

    public Expression rewriteExists(ExistsPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter)
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

    public Expression rewriteIdentifier(Identifier node, C context, ExpressionTreeRewriter<C> treeRewriter)
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

    public Expression rewriteCurrentUser(CurrentUser node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteFieldReference(FieldReference node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSymbolReference(SymbolReference node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteParameter(Parameter node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteQuantifiedComparison(QuantifiedComparisonExpression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteGroupingOperation(GroupingOperation node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBinaryLiteral(BinaryLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBooleanLiteral(BooleanLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCharLiteral(CharLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDecimalLiteral(DecimalLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDoubleLiteral(DoubleLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteGenericLiteral(GenericLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIntervalLiteral(IntervalLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLongLiteral(LongLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteStringLiteral(StringLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTimeLiteral(TimeLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTimestampLiteral(TimestampLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNullLiteral(NullLiteral node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }
}
