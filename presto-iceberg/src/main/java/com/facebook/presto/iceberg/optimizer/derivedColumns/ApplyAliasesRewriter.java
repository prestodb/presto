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
package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Map;

public class ApplyAliasesRewriter
        extends RowExpressionRewriter<Map<VariableReferenceExpression, VariableReferenceExpression>>
{
    @Override
    public RowExpression rewriteVariableReference(
            VariableReferenceExpression variableReferenceExpression,
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            RowExpressionTreeRewriter<Map<VariableReferenceExpression, VariableReferenceExpression>> treeRewriter)
    {
        if (context.containsKey(variableReferenceExpression)) {
            return context.get(variableReferenceExpression);
        }
        return variableReferenceExpression;
    }

    @Override
    public RowExpression rewriteCall(
            CallExpression call,
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            RowExpressionTreeRewriter<Map<VariableReferenceExpression, VariableReferenceExpression>> treeRewriter)
    {
        List<RowExpression> arguments = call.getArguments().stream().map(rowExpression ->
                treeRewriter.rewrite(rowExpression, context)).toList();

        return new CallExpression(call.getSourceLocation(), call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments);
    }

    @Override
    public RowExpression rewriteSpecialForm(
            SpecialFormExpression formExpression,
            Map<VariableReferenceExpression, VariableReferenceExpression> context,
            RowExpressionTreeRewriter<Map<VariableReferenceExpression, VariableReferenceExpression>> treeRewriter)
    {
        List<RowExpression> arguments = formExpression.getArguments().stream().map(rowExpression ->
                treeRewriter.rewrite(rowExpression, context)).toList();
        return new SpecialFormExpression(
                formExpression.getSourceLocation(),
                formExpression.getForm(),
                formExpression.getType(),
                arguments);
    }
}
