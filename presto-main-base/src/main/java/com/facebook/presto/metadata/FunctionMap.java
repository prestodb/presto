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
package com.facebook.presto.metadata;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.SqlFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FunctionMap
{
    private final Multimap<QualifiedObjectName, SqlFunction> functions;

    public FunctionMap()
    {
        functions = ImmutableListMultimap.of();
    }

    public FunctionMap(FunctionMap map, Iterable<? extends SqlFunction> functions)
    {
        requireNonNull(map, "map is null");
        requireNonNull(functions, "functions is null");
        this.functions = ImmutableListMultimap.<QualifiedObjectName, SqlFunction>builder()
                .putAll(map.functions)
                .putAll(Multimaps.index(functions, function -> function.getSignature().getName()))
                .build();

        // Make sure all functions with the same name are aggregations or none of them are
        for (Map.Entry<QualifiedObjectName, Collection<SqlFunction>> entry : this.functions.asMap().entrySet()) {
            Collection<SqlFunction> values = entry.getValue();
            long aggregations = values.stream()
                    .map(function -> function.getSignature().getKind())
                    .filter(kind -> kind == AGGREGATE)
                    .count();
            checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
        }
    }

    public List<SqlFunction> list()
    {
        return ImmutableList.copyOf(functions.values());
    }

    public Collection<SqlFunction> get(QualifiedObjectName name)
    {
        return functions.get(name);
    }
}
