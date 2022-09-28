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

import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.spi.WarningCollector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;

import java.util.Map;
import java.util.Optional;

public interface Predicate
{
    Predicate TRUE = new Predicate()
    {
        @Override
        public boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id, Optional<WarningCollector> warningCollector)
        {
            return true;
        }

        @Override
        public boolean matches(DictionaryDescriptor dictionary)
        {
            return true;
        }

        @Override
        public boolean matches(long numberOfRows, Optional<ColumnIndexStore> columnIndexStore)
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
     * @param id Parquet file name
     * @param warningCollector Presto WarningCollector that is used to collect warnings when the statistics is corrupt
     */
    boolean matches(long numberOfRows, Map<ColumnDescriptor, Statistics<?>> statistics, ParquetDataSourceId id, Optional<WarningCollector> warningCollector);

    /**
     * Should the Parquet Reader process a file section with the specified dictionary based on that
     * single dictionary. This is safe to check repeatedly to avoid loading more parquet dictionaries
     * if the section can already be eliminated.
     *
     * @param dictionary The single column dictionary
     */
    boolean matches(DictionaryDescriptor dictionary);

    /**
     * Should the Parquet Reader process a file section with the specified statistics.
     *
     * @param numberOfRows the number of rows in the segment; this can be used with
     * Statistics to determine if a column is only null
     * @param columnIndexStore column index (statistics) store
     */
    boolean matches(long numberOfRows, Optional<ColumnIndexStore> columnIndexStore);
}
