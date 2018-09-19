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
package com.facebook.presto.hive.statistics;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HivePartitionManager.parsePartition;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.statistics.MetastoreHiveStatisticsProvider.getPartitionsSample;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestMetastoreHiveStatisticsProvider
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");

    private static final HiveColumnHandle PARTITION_COLUMN_1 = new HiveColumnHandle("p1", HIVE_STRING, VARCHAR.getTypeSignature(), 0, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle PARTITION_COLUMN_2 = new HiveColumnHandle("p2", HIVE_LONG, BIGINT.getTypeSignature(), 1, PARTITION_KEY, Optional.empty());

    @Test
    public void testGetPartitionsSample()
    {
        HivePartition p1 = partition("p1=string1/p2=1234");
        HivePartition p2 = partition("p1=string2/p2=2345");
        HivePartition p3 = partition("p1=string3/p2=3456");
        HivePartition p4 = partition("p1=string4/p2=4567");
        HivePartition p5 = partition("p1=string5/p2=5678");

        assertEquals(getPartitionsSample(ImmutableList.of(p1), 1), ImmutableList.of(p1));
        assertEquals(getPartitionsSample(ImmutableList.of(p1), 2), ImmutableList.of(p1));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2), 2), ImmutableList.of(p1, p2));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3), 2), ImmutableList.of(p1, p3));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 1), getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 1));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3), getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3));
        assertThat(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3)).contains(p1, p4);
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4, p5), 3).size(), 3);
    }

    private static HivePartition partition(String name)
    {
        return parsePartition(TABLE, name, ImmutableList.of(PARTITION_COLUMN_1, PARTITION_COLUMN_2), ImmutableList.of(VARCHAR, BIGINT), DateTimeZone.getDefault());
    }
}
