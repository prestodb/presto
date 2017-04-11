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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LambdaCaptureDesugaringRewriter
{
    private final Map<Symbol, Type> symbolTypes;
    private final SymbolAllocator symbolAllocator;

    public LambdaCaptureDesugaringRewriter(Map<Symbol, Type> symbolTypes, SymbolAllocator symbolAllocator)
    {
        this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    }

    public Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression, new Context());
    }

    private static Expression replaceSymbols(Expression expression, ImmutableMap<Symbol, Symbol> symbolMapping)
    {
        return ExpressionTreeRewriter.rewriteWith(
                new ExpressionRewriter<Void>() {
                    @Override
                    public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        Symbol mapTo = symbolMapping.get(new Symbol(node.getName()));
                        if (mapTo == null) {
                            return node;
                        }
                        return mapTo.toSymbolReference();
                    }
                },
                expression);
    }

    public class Visitor
            extends ExpressionRewriter<Context>
    {
        @Override
        public Expression rewriteLambdaExpression(LambdaExpression node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            // Use linked hash set to guarantee deterministic iteration order
            LinkedHashSet<Symbol> referencedSymbols = new LinkedHashSet<>();
            Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), context.withReferencedSymbols(referencedSymbols));

            List<Symbol> lambdaArguments = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Symbol::new)
                    .collect(toImmutableList());

            // referenced symbols - lambda arguments = capture symbols
            // referencedSymbols no longer contains what its name suggests after this line
            referencedSymbols.removeAll(lambdaArguments);
            Set<Symbol> captureSymbols = referencedSymbols;

            // x -> f(x, captureSymbol)    will be rewritten into
            // "$internal$bind"(captureSymbol, (extraSymbol, x) -> f(x, extraSymbol))

            ImmutableMap.Builder<Symbol, Symbol> captureSymbolToExtraSymbol = ImmutableMap.builder();
            ImmutableList.Builder<LambdaArgumentDeclaration> newLambdaArguments = ImmutableList.builder();
            for (Symbol captureSymbol : captureSymbols) {
                Symbol extraSymbol = symbolAllocator.newSymbol(captureSymbol.getName(), symbolTypes.get(captureSymbol));
                captureSymbolToExtraSymbol.put(captureSymbol, extraSymbol);
                newLambdaArguments.add(new LambdaArgumentDeclaration(extraSymbol.getName()));
            }
            newLambdaArguments.addAll(node.getArguments());
            Expression rewrittenExpression = new LambdaExpression(newLambdaArguments.build(), replaceSymbols(rewrittenBody, captureSymbolToExtraSymbol.build()));
            for (Symbol captureSymbol : captureSymbols) {
                rewrittenExpression = new BindExpression(new SymbolReference(captureSymbol.getName()), rewrittenExpression);
            }

            context.getReferencedSymbols().addAll(captureSymbols);
            return rewrittenExpression;
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            context.getReferencedSymbols().add(new Symbol(node.getName()));
            return null;
        }
    }

    private static class Context
    {
        // Use linked hash set to guarantee deterministic iteration order
        LinkedHashSet<Symbol> referencedSymbols;

        public Context()
        {
            this(new LinkedHashSet<>());
        }

        private Context(LinkedHashSet<Symbol> referencedSymbols)
        {
            this.referencedSymbols = referencedSymbols;
        }

        public LinkedHashSet<Symbol> getReferencedSymbols()
        {
            return referencedSymbols;
        }

        public Context withReferencedSymbols(LinkedHashSet<Symbol> symbols)
        {
            return new Context(symbols);
        }
    }
}
