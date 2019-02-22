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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static java.lang.String.format;

public class TableExpressionRewriter<R, C>
{
    private List<RewriteRule<? extends TableExpression, R, C>> baseRules = new ArrayList<>();
    private Map<Class<?>, RewriteRule<? extends TableExpression, R, C>> finalRules = new HashMap<>();

    public <T extends TableExpression> TableExpressionRewriter<R, C> addRule(Class<T> clazz, TriFunction<TableExpressionRewriter<R, C>, T, C, R> function)
    {
        if (Modifier.isFinal(clazz.getModifiers())) {
            finalRules.put(clazz, new RewriteRule<>(clazz, function));
        }
        else {
            baseRules.add(new RewriteRule<>(clazz, function));
        }
        return this;
    }

    public <T extends TableExpression> TableExpressionRewriter<R, C> addRule(Class<T> clazz, BiFunction<T, C, R> function)
    {
        if (Modifier.isFinal(clazz.getModifiers())) {
            finalRules.put(clazz, new RewriteRule<>(clazz, (rewriter, node, context) -> function.apply(node, context)));
        }
        else {
            baseRules.add(new RewriteRule<>(clazz, (rewriter, node, context) -> function.apply(node, context)));
        }
        return this;
    }

    public R accept(TableExpression node, C context)
    {
        if (finalRules.containsKey(node.getClass())) {
            return finalRules.get(node.getClass()).apply(this, node, context);
        }
        Optional<RewriteRule<? extends TableExpression, R, C>> rule = baseRules.stream()
                .filter(x -> x.match(node))
                .findFirst();
        if (rule.isPresent()) {
            return rule.get().apply(this, node, context);
        }
        throw new UnsupportedOperationException(format("No rules provided to visit node type %s", node.getClass().getName()));
    }
}
