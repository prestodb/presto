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
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class RowExpressionTreeRewriter<C>
{
    private final RowExpressionRewriter<C> rewriter;
    private final RowExpressionVisitor<RowExpression, Context<C>> visitor;

    public static <C, T extends RowExpression> T rewriteWith(RowExpressionRewriter<C> rewriter, T node)
    {
        return new RowExpressionTreeRewriter<>(rewriter).rewrite(node, null);
    }

    public static <C, T extends RowExpression> T rewriteWith(RowExpressionRewriter<C> rewriter, T node, C context)
    {
        return new RowExpressionTreeRewriter<>(rewriter).rewrite(node, context);
    }

    public RowExpressionTreeRewriter(RowExpressionRewriter<C> rewriter)
    {
        this.rewriter = rewriter;
        this.visitor = new RewritingVisitor();
    }

    private List<RowExpression> rewrite(List<RowExpression> items, Context<C> context)
    {
        List<RowExpression> rewritenExpressions = new ArrayList<>();
        for (RowExpression expression : items) {
            rewritenExpressions.add(rewrite(expression, context.get()));
        }
        return Collections.unmodifiableList(rewritenExpressions);
    }

    @SuppressWarnings("unchecked")
    public <T extends RowExpression> T rewrite(T node, C context)
    {
        return (T) node.accept(visitor, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for the provided node.
     */
    @SuppressWarnings("unchecked")
    public <T extends RowExpression> T defaultRewrite(T node, C context)
    {
        return (T) node.accept(visitor, new Context<>(context, true));
    }

    private class RewritingVisitor
            implements RowExpressionVisitor<RowExpression, Context<C>>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression input, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowExpression result = rewriter.rewriteInputReference(input, context.get(), RowExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return input;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowExpression result = rewriter.rewriteCall(call, context.get(), RowExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<RowExpression> arguments = rewrite(call.getArguments(), context);

            if (!sameElements(call.getArguments(), arguments)) {
                return new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments);
            }
            return call;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowExpression result = rewriter.rewriteConstant(literal, context.get(), RowExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowExpression result = rewriter.rewriteLambda(lambda, context.get(), RowExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            RowExpression body = rewrite(lambda.getBody(), context.get());
            if (body != lambda.getBody()) {
                return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), body);
            }

            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression variable, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowExpression result = rewriter.rewriteVariableReference(variable, context.get(), RowExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return variable;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                RowExpression result = rewriter.rewriteSpecialForm(specialForm, context.get(), RowExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<RowExpression> arguments = rewrite(specialForm.getArguments(), context);

            if (!sameElements(specialForm.getArguments(), arguments)) {
                return new SpecialFormExpression(specialForm.getForm(), specialForm.getType(), arguments);
            }
            return specialForm;
        }
    }

    public static class Context<C>
    {
        private final boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
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
