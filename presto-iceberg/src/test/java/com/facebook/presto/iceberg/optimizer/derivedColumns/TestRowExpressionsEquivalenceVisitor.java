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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;

@Test
public class TestRowExpressionsEquivalenceVisitor
        extends AbstractTestDistributedQueries
{
    public void testGeneratedRowExpressionAreEqual()
    {
        ImmutableMap<String, ColumnMetadata> context = ImmutableMap.of("c2", ColumnMetadata.builder().setName("c2").setType(VarcharType.VARCHAR).build());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setExtraConnectorProperties(ImmutableMap.of("iceberg.hive-statistics-merge-strategy", Joiner.on(",").join(NUMBER_OF_DISTINCT_VALUES.name(), TOTAL_SIZE_IN_BYTES.name())))
                // These tests do not rely on long query history (no assertions on past queries,
                // retries, or timing behavior). The aggressive limits below are chosen solely to
                // reduce query history memory usage and are safe for all Iceberg distributed tests.
                .setExtraProperties(ImmutableMap.of("query.max-age", "10s",
                        "query.max-history", "10"))
                .build().getQueryRunner();
    }
}
