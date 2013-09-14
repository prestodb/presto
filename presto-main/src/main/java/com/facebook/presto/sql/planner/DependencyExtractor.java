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

import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

// TODO: a similar class exists in (TupleAnalyzer.DependencyExtractor)
public class DependencyExtractor
{
    public static Set<Symbol> extractUnique(Expression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static List<Symbol> extractAll(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new Visitor().process(expression, builder);
        return builder.build();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableList.Builder<Symbol> builder)
        {
            builder.add(Symbol.fromQualifiedName(node.getName()));
            return null;
        }
    }
}
