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
package com.facebook.presto.sql.planner.iterative.connector.rewriter;

import com.facebook.presto.spi.relation.TableExpression;

import static java.util.Objects.requireNonNull;

public class RewriteRule<T, R, C>
{
    private final Class<T> clazz;
    private final TriFunction<TableExpressionRewriter<R, C>, T, C, R> function;

    public RewriteRule(Class<T> clazz, TriFunction<TableExpressionRewriter<R, C>, T, C, R> function)
    {
        this.clazz = requireNonNull(clazz, "clazz is null");
        this.function = requireNonNull(function, "function is null");
    }

    public boolean match(TableExpression node)
    {
        return clazz.isInstance(node);
    }

    public R apply(TableExpressionRewriter<R, C> rewriter, TableExpression tableExpression, C context)
    {
        return function.apply(rewriter, clazz.cast(tableExpression), context);
    }
}
