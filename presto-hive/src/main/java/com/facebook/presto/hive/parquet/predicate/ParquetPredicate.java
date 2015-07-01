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
package com.facebook.presto.hive.parquet.predicate;

import parquet.column.statistics.Statistics;

import java.util.Map;

public interface ParquetPredicate
{
    ParquetPredicate TRUE = new ParquetPredicate()
    {
        @Override
        public boolean matches(long numberOfRows, Map<Integer, Statistics<?>> statisticsByColumnIndex)
        {
            return true;
        }

        @Override
        public boolean matches(Map<Integer, ParquetDictionaryDescriptor> dictionariesByColumnIndex)
        {
            return true;
        }
    };

    /**
     * Should the Parquet Reader process a file section with the specified statistics.
     *
     * @param numberOfRows the number of rows in the segment; this can be used with
     * Statistics to determine if a column is only null
     * @param statisticsByColumnIndex statistics for column by ordinal position
     * in the file; this will match the field order from the hive metastore
     */
    boolean matches(long numberOfRows, Map<Integer, Statistics<?>> statisticsByColumnIndex);

    /**
     * Should the Parquet Reader process a file section with the specified dictionary.
     *
     * @param dictionariesByColumnIndex dictionaries for column by ordinal position
     * in the file; this will match the field order from the hive metastore
     */
    boolean matches(Map<Integer, ParquetDictionaryDescriptor> dictionariesByColumnIndex);
}
