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
package io.prestosql.plugin.jmx;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toSet;

public class JmxHistoricalData
{
    private final Set<String> tables;
    private final Map<String, EvictingQueue<List<Object>>> tableData = new HashMap<>();

    @Inject
    public JmxHistoricalData(JmxConnectorConfig jmxConfig)
    {
        this(jmxConfig.getMaxEntries(), jmxConfig.getDumpTables());
    }

    public JmxHistoricalData(int maxEntries, Set<String> tableNames)
    {
        tables = tableNames.stream()
                .map(tableName -> tableName.toLowerCase(Locale.ENGLISH))
                .collect(toSet());
        for (String tableName : tables) {
            tableData.put(tableName, EvictingQueue.create(maxEntries));
        }
    }

    public Set<String> getTables()
    {
        return tables;
    }

    public synchronized void addRow(String tableName, List<Object> row)
    {
        String lowerCaseTableName = tableName.toLowerCase(Locale.ENGLISH);
        checkArgument(tableData.containsKey(lowerCaseTableName));
        tableData.get(lowerCaseTableName).add(row);
    }

    public synchronized List<List<Object>> getRows(String objectName, List<Integer> selectedColumns)
    {
        String lowerCaseObjectName = objectName.toLowerCase(Locale.ENGLISH);
        if (!tableData.containsKey(lowerCaseObjectName)) {
            return ImmutableList.of();
        }
        return projectRows(tableData.get(lowerCaseObjectName), selectedColumns);
    }

    private List<List<Object>> projectRows(Collection<List<Object>> rows, List<Integer> selectedColumns)
    {
        ImmutableList.Builder<List<Object>> result = ImmutableList.builder();
        for (List<Object> row : rows) {
            List<Object> projectedRow = new ArrayList<>();
            for (Integer selectedColumn : selectedColumns) {
                projectedRow.add(row.get(selectedColumn));
            }
            result.add(projectedRow);
        }
        return result.build();
    }
}
