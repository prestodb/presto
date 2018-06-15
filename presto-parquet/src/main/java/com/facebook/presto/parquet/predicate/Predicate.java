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
package com.facebook.presto.parquet.predicate;

import parquet.column.ColumnDescriptor;
import parquet.column.statistics.Statistics;

import java.util.Map;

public interface Predicate
{
    Predicate TRUE = new Predicate()
    {
        @Override
        public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics)
        {
            return true;
        }

        @Override
        public boolean matches(Map<ColumnDescriptor, DictionaryDescriptor> dictionaries)
        {
            return true;
        }
    };

    /**
     * Should the Parquet Reader process a file section with the specified statistics.
     *
     * @param numberOfRows the number of rows in the segment; this can be used with
     * Statistics to determine if a column is only null
     * @param statistics column statistics
     */
    boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics);

    /**
     * Should the Parquet Reader process a file section with the specified dictionary.
     *
     * @param dictionaries dictionaries per column
     */
    boolean matches(Map<ColumnDescriptor, DictionaryDescriptor> dictionaries);
}
