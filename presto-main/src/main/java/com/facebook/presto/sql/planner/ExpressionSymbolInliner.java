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
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class ExpressionSymbolInliner
        extends ExpressionRewriter<Void>
{
    private final Map<Symbol, ? extends Expression> mappings;

    public ExpressionSymbolInliner(Map<Symbol, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Expression expression = mappings.get(Symbol.from(node));
        checkState(expression != null, "Cannot resolve symbol %s", node.getName());
        return expression;
    }
}
