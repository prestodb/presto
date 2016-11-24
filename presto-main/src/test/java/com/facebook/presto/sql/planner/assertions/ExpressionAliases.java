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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class ExpressionAliases
{
    private final Multimap<String, Expression> map;

    public ExpressionAliases()
    {
        this.map = ArrayListMultimap.create();
    }

    public ExpressionAliases(ExpressionAliases expressionAliases)
    {
        requireNonNull(expressionAliases, "symbolAliases are null");
        this.map = ArrayListMultimap.create(expressionAliases.map);
    }

    public void put(String alias, Expression expression)
    {
        alias = alias(alias);
        if (map.containsKey(alias)) {
            checkState(map.get(alias).contains(expression), "Alias '%s' points to different expression '%s' and '%s'", alias, expression, map.get(alias));
        }
        else {
            checkState(!map.values().contains(expression), "Expression '%s' is already pointed by different alias than '%s', check mapping for '%s'", expression, alias, map);
            map.put(alias, expression);
        }
    }

    private static String alias(String alias)
    {
        return alias.toLowerCase().replace("(", "").replace(")", "").replace("\"", "");
    }

    public void updateAssignments(Map<Symbol, Expression> assignments)
    {
        ImmutableMultimap.Builder<String, Expression> mapUpdate = ImmutableMultimap.builder();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            for (String alias : map.keys()) {
                if (map.get(alias).contains(assignment.getKey().toSymbolReference())) {
                    mapUpdate.put(alias, assignment.getValue());
                }
            }
        }
        map.putAll(mapUpdate.build());
    }
}
