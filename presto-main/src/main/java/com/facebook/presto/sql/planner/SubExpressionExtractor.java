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

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Extracts and returns the set of all expression subtrees within an Expression, including Expression itself
 */
public final class SubExpressionExtractor
{
    private SubExpressionExtractor() {}

    public static List<Expression> extractAll(Iterable<Expression> expressions)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression expression : expressions) {
            extract(builder, expression);
        }
        return builder.build();
    }

    public static List<Expression> extractAll(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        extract(builder, expression);
        return builder.build();
    }

    public static Set<Expression> extract(Expression expression)
    {
        final ImmutableSet.Builder<Expression> builder = ImmutableSet.builder();
        extract(builder, expression);
        return builder.build();
    }

    private static void extract(final ImmutableCollection.Builder<Expression> builder, Expression expression)
    {
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            public Void process(Node node, @Nullable Void context)
            {
                Expression expression = (Expression) node;
                builder.add(expression);

                return super.process(node, context);
            }
        }.process(expression, null);
    }
}
