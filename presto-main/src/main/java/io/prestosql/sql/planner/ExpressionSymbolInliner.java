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
package io.prestosql.sql.planner;

import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public final class ExpressionSymbolInliner
{
    public static Expression inlineSymbols(Map<Symbol, ? extends Expression> mapping, Expression expression)
    {
        return inlineSymbols(mapping::get, expression);
    }

    public static Expression inlineSymbols(Function<Symbol, Expression> mapping, Expression expression)
    {
        return new ExpressionSymbolInliner(mapping).rewrite(expression);
    }

    private final Function<Symbol, Expression> mapping;

    private ExpressionSymbolInliner(Function<Symbol, Expression> mapping)
    {
        this.mapping = mapping;
    }

    private Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Set<String> excludedNames = new HashSet<>();

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (excludedNames.contains(node.getName())) {
                return node;
            }

            Expression expression = mapping.apply(Symbol.from(node));
            checkState(expression != null, "Cannot resolve symbol %s", node.getName());
            return expression;
        }

        @Override
        public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            for (LambdaArgumentDeclaration argument : node.getArguments()) {
                String argumentName = argument.getName().getValue();
                // Symbol names are unique. As a result, a symbol should never be excluded multiple times.
                checkArgument(!excludedNames.contains(argumentName));
                excludedNames.add(argumentName);
            }
            Expression result = treeRewriter.defaultRewrite(node, context);
            for (LambdaArgumentDeclaration argument : node.getArguments()) {
                excludedNames.remove(argument.getName().getValue());
            }
            return result;
        }
    }
}
