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
package com.facebook.presto.tpch;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class TpchIndexSpec
{
    public static final TpchIndexSpec NO_INDEXES = new TpchIndexSpec(ImmutableSetMultimap.<Table, Set<String>>of());

    private final SetMultimap<Table, Set<String>> spec;

    private TpchIndexSpec(SetMultimap<Table, Set<String>> spec)
    {
        this.spec = checkNotNull(spec, "spec is null");
    }

    public Set<Table> listIndexedTables()
    {
        return spec.keySet();
    }

    public Iterable<Set<String>> getColumnIndexes(Table table)
    {
        return spec.get(table);
    }

    public static class Builder
    {
        private final ImmutableSetMultimap.Builder<Table, Set<String>> builder = ImmutableSetMultimap.builder();

        public Builder addIndex(String tableName, double scaleFactor, Set<String> columnNames)
        {
            builder.put(new Table(tableName, scaleFactor), columnNames);
            return this;
        }

        public TpchIndexSpec build()
        {
            return new TpchIndexSpec(builder.build());
        }
    }

    public static class Table
    {
        private final String tableName;
        private final double scaleFactor;

        public Table(String tableName, double scaleFactor)
        {
            this.tableName = checkNotNull(tableName, "tableName is null");
            this.scaleFactor = scaleFactor;
        }

        public String getTableName()
        {
            return tableName;
        }

        public double getScaleFactor()
        {
            return scaleFactor;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, scaleFactor);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final Table other = (Table) obj;
            return Objects.equals(this.tableName, other.tableName) && Objects.equals(this.scaleFactor, other.scaleFactor);
        }
    }
}
