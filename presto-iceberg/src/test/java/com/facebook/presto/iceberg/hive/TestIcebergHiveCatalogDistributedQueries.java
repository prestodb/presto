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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.iceberg.TestIcebergDistributedQueries;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;

public class TestIcebergHiveCatalogDistributedQueries
        extends TestIcebergDistributedQueries
{
    public TestIcebergHiveCatalogDistributedQueries()
    {
        super(HIVE, ImmutableMap.of("iceberg.hive-statistics-merge-strategy", Joiner.on(",").join(NUMBER_OF_DISTINCT_VALUES.name(), TOTAL_SIZE_IN_BYTES.name())));
    }
}
