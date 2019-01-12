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
package io.prestosql.tests.tpch;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TpchIndexSpec
{
    public static final TpchIndexSpec NO_INDEXES = new TpchIndexSpec(ImmutableSetMultimap.of());

    private final SetMultimap<TpchScaledTable, Set<String>> spec;

    private TpchIndexSpec(SetMultimap<TpchScaledTable, Set<String>> spec)
    {
        this.spec = ImmutableSetMultimap.copyOf(requireNonNull(spec, "spec is null"));
    }

    public Set<TpchScaledTable> listIndexedTables()
    {
        return spec.keySet();
    }

    public Iterable<Set<String>> getColumnIndexes(TpchScaledTable table)
    {
        return spec.get(table);
    }

    public static class Builder
    {
        private final ImmutableSetMultimap.Builder<TpchScaledTable, Set<String>> builder = ImmutableSetMultimap.builder();

        public Builder addIndex(String tableName, double scaleFactor, Set<String> columnNames)
        {
            builder.put(new TpchScaledTable(tableName, scaleFactor), columnNames);
            return this;
        }

        public TpchIndexSpec build()
        {
            return new TpchIndexSpec(builder.build());
        }
    }
}
