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

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;

public class StringTableUtils
{
    public static String getShortestTableStringFormat(List<List<String>> table)
            throws IllegalArgumentException
    {
        if (table.size() == 0) {
            return "";
        }

        int tableWidth = table.get(0).size();
        int[] lengthTracker = new int[tableWidth];
        for (List<String> row : table) {
            if (row.size() != lengthTracker.length) {
                throw new IllegalArgumentException("all lists in table must be the same size");
            }

            for (int i = 0; i < row.size(); i++) {
                lengthTracker[i] = max(row.get(i).length(), lengthTracker[i]);
            }
        }

        String formatString = "|";
        for (int maxLen : lengthTracker) {
            formatString += " %-" + maxLen + "s |";
        }

        return formatString.trim();
    }

    public static List<String> getTableStrings(List<List<String>> table)
            throws IllegalArgumentException
    {
        String formatString = getShortestTableStringFormat(table);
        ArrayList<String> outputList = new ArrayList<String>(table.size());
        for (int i = 0; i < table.size(); i++) {
            outputList.add(String.format(formatString, table.get(i).toArray()));
        }

        return outputList;
    }

    private StringTableUtils() {}
}
