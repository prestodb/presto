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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;

import java.util.function.Function;

public class SymbolToSymbolTranslator
        extends RowExpressionRewriter<Function<Symbol, Symbol>>
{
    private SymbolToSymbolTranslator()
    {
    }

    public static RowExpression rewriteWith(RowExpression input, Function<Symbol, Symbol> mapping)
    {
        return RowExpressionTreeRewriter.rewriteWith(new SymbolToSymbolTranslator(), input, mapping);
    }

    @Override
    public RowExpression rewriteVariableReference(VariableReferenceExpression node, Function<Symbol, Symbol> context, RowExpressionTreeRewriter<Function<Symbol, Symbol>> treeRewriter)
    {
        Symbol transformed = context.apply(new Symbol(node.getName()));
        if (!transformed.getName().equals(node.getName())) {
            return new VariableReferenceExpression(transformed.getName(), node.getType());
        }
        return null;
    }
}
