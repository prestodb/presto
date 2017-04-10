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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SymbolToInputRewriter
{
    private final Map<Symbol, Integer> symbolToChannelMapping;

    public SymbolToInputRewriter(Map<Symbol, Integer> symbolToChannelMapping)
    {
        requireNonNull(symbolToChannelMapping, "symbolToChannelMapping is null");
        this.symbolToChannelMapping = ImmutableMap.copyOf(symbolToChannelMapping);
    }

    public Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Context>() {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
            {
                Integer channel = symbolToChannelMapping.get(Symbol.from(node));
                if (channel == null) {
                    Preconditions.checkArgument(context.isInLambda(), "Cannot resolve symbol %s", node.getName());
                    return node;
                }
                return new FieldReference(channel);
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
            {
                return treeRewriter.defaultRewrite(node, new Context(true));
            }
        }, expression, new Context(false));
    }

    private static class Context
    {
        private final boolean inLambda;

        public Context(boolean inLambda)
        {
            this.inLambda = inLambda;
        }

        public boolean isInLambda()
        {
            return inLambda;
        }
    }
}
