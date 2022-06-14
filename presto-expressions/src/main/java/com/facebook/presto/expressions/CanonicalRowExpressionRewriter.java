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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class CanonicalRowExpressionRewriter
        extends RowExpressionRewriter<Void>
{
    private static final CanonicalRowExpressionRewriter SINGLETON = new CanonicalRowExpressionRewriter();

    private CanonicalRowExpressionRewriter() {}

    public static RowExpression canonicalizeRowExpression(RowExpression expression)
    {
        return RowExpressionTreeRewriter.rewriteWith(SINGLETON, expression, null);
    }

    @Override
    public RowExpression rewriteInputReference(InputReferenceExpression input, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        return input.canonicalize();
    }

    @Override
    public RowExpression rewriteCall(CallExpression call, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        List<RowExpression> arguments = rewrite(call.getArguments(), context, treeRewriter);

        if (!sameElements(call.getArguments(), arguments)) {
            return new CallExpression(Optional.empty(), call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments);
        }
        return call.canonicalize();
    }

    @Override
    public RowExpression rewriteConstant(ConstantExpression literal, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        return literal.canonicalize();
    }

    @Override
    public RowExpression rewriteLambda(LambdaDefinitionExpression lambda, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        RowExpression body = treeRewriter.rewrite(lambda.getBody(), context);
        if (body != lambda.getBody()) {
            return new LambdaDefinitionExpression(Optional.empty(), lambda.getArgumentTypes(), lambda.getArguments(), body);
        }

        return lambda.canonicalize();
    }

    @Override
    public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        return variable.canonicalize();
    }

    @Override
    public RowExpression rewriteSpecialForm(SpecialFormExpression specialForm, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        List<RowExpression> arguments = rewrite(specialForm.getArguments(), context, treeRewriter);

        if (!sameElements(specialForm.getArguments(), arguments)) {
            return new SpecialFormExpression(Optional.empty(), specialForm.getForm(), specialForm.getType(), arguments);
        }
        return specialForm.canonicalize();
    }

    private List<RowExpression> rewrite(List<RowExpression> items, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        List<RowExpression> rewrittenExpressions = new ArrayList<>();
        for (RowExpression expression : items) {
            rewrittenExpressions.add(treeRewriter.rewrite(expression, context));
        }
        return Collections.unmodifiableList(rewrittenExpressions);
    }

    @SuppressWarnings("ObjectEquality")
    private static <T> boolean sameElements(Collection<? extends T> a, Collection<? extends T> b)
    {
        if (a.size() != b.size()) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }
}
