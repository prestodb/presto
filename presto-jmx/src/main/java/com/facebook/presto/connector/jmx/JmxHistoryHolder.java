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
package com.facebook.presto.connector.jmx;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class JmxHistoryHolder
{
    private final Set<String> tables;
    private final Map<String, EvictingQueue<List<Object>>> tablesRows = new HashMap<>();

    public JmxHistoryHolder(int evictionLimit, Set<String> tableNames)
    {
        tables = ImmutableSet.copyOf(tableNames);
        for (String tableName : tableNames) {
            tablesRows.put(tableName, EvictingQueue.create(evictionLimit));
        }
    }

    public Set<String> getTables()
    {
        return tables;
    }

    public synchronized void addRow(String tableName, List<Object> row)
    {
        checkArgument(tablesRows.containsKey(tableName));
        tablesRows.get(tableName).add(row);
    }

    public synchronized List<List<Object>> getRows(String tableName, List<Integer> selectedColumns)
    {
        if (!tablesRows.containsKey(tableName)) {
            return ImmutableList.of();
        }
        return projectRows(tablesRows.get(tableName), selectedColumns);
    }

    private List<List<Object>> projectRows(Collection<List<Object>> rows, List<Integer> selectedColumns)
    {
        ImmutableList.Builder<List<Object>> result = ImmutableList.builder();
        for (List<Object> row : rows) {
            ImmutableList.Builder<Object> projectedRow = ImmutableList.builder();
            for (Integer selectedColumn : selectedColumns) {
                projectedRow.add(row.get(selectedColumn));
            }
            result.add(projectedRow.build());
        }
        return result.build();
    }
}
