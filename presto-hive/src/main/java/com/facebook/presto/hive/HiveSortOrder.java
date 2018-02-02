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
package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HiveSortOrder
{
    public enum Ordering
    {
        ASCENDING('+'),
        DESCENDING('-');

        private final char prefix;

        Ordering(char prefix)
        {
            this.prefix = prefix;
        }

        public char getPrefix()
        {
            return prefix;
        }
    }

    private final String columnName;
    private final Ordering ordering;

    @JsonCreator
    public HiveSortOrder(
            @JsonProperty("column") String columnName,
            @JsonProperty("ordering") Ordering ordering)
    {
        this.columnName = columnName;
        this.ordering = ordering;
    }

    @JsonProperty("column")
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty("ordering")
    public Ordering getOrdering()
    {
        return ordering;
    }
}
