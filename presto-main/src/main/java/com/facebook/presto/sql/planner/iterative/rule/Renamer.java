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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

class Renamer
{
    private final Map<Symbol, Symbol> renames;

    public static Renamer from(ProjectNode node)
    {
        ImmutableMap.Builder<Symbol, Symbol> result = ImmutableMap.builder();

        for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            Expression value = entry.getValue();

            if (value instanceof SymbolReference && !((SymbolReference) value).getName().equals(symbol.getName())) {
                result.put(symbol, Symbol.from(value));
            }
        }

        return new Renamer(result.build());
    }

    private Renamer(Map<Symbol, Symbol> renames)
    {
        this.renames = renames;
    }

    public boolean hasRenames()
    {
        return !renames.isEmpty();
    }

    public Symbol rename(Symbol symbol)
    {
        return renames.getOrDefault(symbol, symbol);
    }

    public Expression rename(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Object>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference reference, Object context, ExpressionTreeRewriter treeRewriter)
            {
                return rename(Symbol.from(reference)).toSymbolReference();
            }
        }, expression);
    }

    public ProjectNode renameOutputs(ProjectNode node)
    {
        Assignments.Builder assignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
            assignments.put(
                    rename(assignment.getKey()),
                    assignment.getValue());
        }

        return new ProjectNode(node.getId(), node.getSource(), assignments.build());
    }
}
