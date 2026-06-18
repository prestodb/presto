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
package com.facebook.presto.expressions;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

public class RowExpressionRewriter<C>
{
    public RowExpression rewriteRowExpression(RowExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public RowExpression rewriteInputReference(InputReferenceExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteCall(CallExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteConstant(ConstantExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteLambda(LambdaDefinitionExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteVariableReference(VariableReferenceExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteSpecialForm(SpecialFormExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }
}
