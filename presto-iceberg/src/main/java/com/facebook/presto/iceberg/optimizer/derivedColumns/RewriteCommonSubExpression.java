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

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class RewriteCommonSubExpression
        implements RowExpressionVisitor<RewrittenExpressionMetadata, TreeMap<RowExpression, RowExpression>>
{
    @Override
    public RewrittenExpressionMetadata visitExpression(RowExpression expression, TreeMap<RowExpression, RowExpression> context)
    {
        return new RewrittenExpressionMetadata(expression, List.of());
    }

    @Override
    public RewrittenExpressionMetadata visitCall(CallExpression call, TreeMap<RowExpression, RowExpression> context)
    {
        checkArgument(context != null);
        if (context.containsKey(call.canonicalize())) {
            RowExpression rewrittenExpression = context.get(call.canonicalize());
            checkState(rewrittenExpression instanceof VariableReferenceExpression, "Derived column must be a VariableReferenceExpression");
            VariableReferenceExpression derivedColumnAdded = ((VariableReferenceExpression) rewrittenExpression);
            return new RewrittenExpressionMetadata(rewrittenExpression, ImmutableList.of(derivedColumnAdded));
        }
        List<RewrittenExpressionMetadata> arguments = call.getArguments().stream().map(rowExpression -> rowExpression.accept(this, context)).toList();
        List<VariableReferenceExpression> derivedColumnsAdded = arguments.stream()
                .map(RewrittenExpressionMetadata::derivedColumnsAdded)
                .reduce((list1, list2) ->
                        ImmutableList.<VariableReferenceExpression>builder().addAll(list1).addAll(list2).build()).orElse(ImmutableList.of());
        CallExpression callExpression = new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(),
                arguments.stream().map(RewrittenExpressionMetadata::rewrittenExpression).toList());
        return new RewrittenExpressionMetadata(callExpression, derivedColumnsAdded);
    }

    @Override
    public RewrittenExpressionMetadata visitSpecialForm(SpecialFormExpression specialForm, TreeMap<RowExpression, RowExpression> context)
    {
        checkArgument(context != null);
        if (context.containsKey(specialForm.canonicalize())) {
            RowExpression rewrittenExpression = context.get(specialForm.canonicalize());
            checkState(rewrittenExpression instanceof VariableReferenceExpression, "Derived column must be a VariableReferenceExpression");
            VariableReferenceExpression derivedColumnAdded = ((VariableReferenceExpression) rewrittenExpression);
            return new RewrittenExpressionMetadata(rewrittenExpression, ImmutableList.of(derivedColumnAdded));
        }
        List<RewrittenExpressionMetadata> arguments = specialForm.getArguments().stream().map(rowExpression -> rowExpression.accept(this, context)).toList();
        List<VariableReferenceExpression> derivedColumnsAdded = arguments.stream()
                .map(RewrittenExpressionMetadata::derivedColumnsAdded)
                .reduce((list1, list2) ->
                        ImmutableList.<VariableReferenceExpression>builder().addAll(list1).addAll(list2).build()).orElse(ImmutableList.of());
        SpecialFormExpression specialFormExpression = new SpecialFormExpression(specialForm.getForm(), specialForm.getType(),
                arguments.stream().map(RewrittenExpressionMetadata::rewrittenExpression).toList());
        return new RewrittenExpressionMetadata(specialFormExpression, derivedColumnsAdded);
    }
}
