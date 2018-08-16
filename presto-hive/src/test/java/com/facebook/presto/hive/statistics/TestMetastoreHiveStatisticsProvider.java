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

import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.statistics.MetastoreHiveStatisticsProvider.getPartitionsSample;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestMetastoreHiveStatisticsProvider
{
    @Test
    public void testGetPartitionsSample()
    {
        assertEquals(getPartitionsSample(ImmutableList.of(partition("p1")), 1), ImmutableList.of(partition("p1")));
        assertEquals(getPartitionsSample(ImmutableList.of(partition("p1")), 2), ImmutableList.of(partition("p1")));
        assertEquals(
                getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2")), 2),
                ImmutableList.of(partition("p1"), partition("p2")));
        assertEquals(
                getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2"), partition("p3")), 2),
                ImmutableList.of(partition("p1"), partition("p3")));
        assertEquals(
                getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2"), partition("p3"), partition("p4")), 1),
                getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2"), partition("p3"), partition("p4")), 1));
        assertEquals(
                getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2"), partition("p3"), partition("p4")), 3),
                getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2"), partition("p3"), partition("p4")), 3));
        assertThat(getPartitionsSample(ImmutableList.of(partition("p1"), partition("p2"), partition("p3"), partition("p4")), 3))
                .contains(partition("p1"), partition("p3"));
    }

    private static HivePartition partition(String name)
    {
        return new HivePartition(new SchemaTableName("schema", "table"), name, ImmutableMap.of());
    }
}
