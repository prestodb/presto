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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/*
 * TODO Eventually we should only need to inline VariableReferenceExpression to RowExpression. We should remove this once https://github.com/prestodb/presto/issues/12828 is done.
 */
public class ExpressionVariableInliner
{
    public static Expression inlineVariables(Map<VariableReferenceExpression, ? extends Expression> mapping, Expression expression, TypeProvider types)
    {
        return inlineVariables(mapping::get, expression, types);
    }

    public static Expression inlineVariables(Function<VariableReferenceExpression, Expression> mapping, Expression expression, TypeProvider types)
    {
        return new ExpressionVariableInliner(mapping, types).rewrite(expression);
    }

    private final Function<VariableReferenceExpression, Expression> mapping;
    private final TypeProvider types;

    private ExpressionVariableInliner(Function<VariableReferenceExpression, Expression> mapping, TypeProvider types)
    {
        this.mapping = mapping;
        this.types = types;
    }

    private Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionVariableInliner.Visitor(), expression);
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

            Expression expression = mapping.apply(new VariableReferenceExpression(node.getName(), types.get(node)));
            checkState(expression != null, "Cannot resolve symbol %s", node.getName());
            return expression;
        }

        @Override
        public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            for (LambdaArgumentDeclaration argument : node.getArguments()) {
                String argumentName = argument.getName().getValue();
                // Variable names are unique. As a result, a variable should never be excluded multiple times.
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
