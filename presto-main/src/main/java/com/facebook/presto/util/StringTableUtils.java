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
package com.facebook.presto.util;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.String.format;

public class StringTableUtils
{
    private StringTableUtils() {}

    public static String getShortestTableStringFormat(List<List<String>> table)
    {
        if (table.size() == 0) {
            throw new IllegalArgumentException("Table must include at least one row");
        }

        int tableWidth = table.get(0).size();
        int[] lengthTracker = new int[tableWidth];
        for (List<String> row : table) {
            if (row.size() != tableWidth) {
                String errorString = format("All rows in the table are expected to have exactly same number of columns: %s != %s",
                        tableWidth, row.size());
                throw new IllegalArgumentException(errorString);
            }

            for (int i = 0; i < row.size(); i++) {
                lengthTracker[i] = max(row.get(i).length(), lengthTracker[i]);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int maxLen : lengthTracker) {
            sb.append(" %-")
                    .append(maxLen)
                    .append("s |");
        }

        return sb.toString();
    }

    public static List<String> getTableStrings(List<List<String>> table)
    {
        String formatString = getShortestTableStringFormat(table);
        return table.stream()
                .map(List::toArray)
                .map(line -> format(formatString, line))
                .collect(toImmutableList());
    }
}
