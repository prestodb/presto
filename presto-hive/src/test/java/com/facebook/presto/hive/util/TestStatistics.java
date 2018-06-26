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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.Statistics.ReduceOperator;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveBasicStatistics.createFromPartitionParameters;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestPartition;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestTable;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.SUBTRACT;
import static com.facebook.presto.hive.util.Statistics.reduce;
import static com.facebook.presto.hive.util.Statistics.updateStatistics;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatistics
{
    @Test
    public void testReduce()
    {
        assertThat(reduce(createEmptyStatistics(), createEmptyStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createZeroStatistics(), createEmptyStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createZeroStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createEmptyStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createZeroStatistics(), createEmptyStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createZeroStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4), ADD))
                .isEqualTo(new HiveBasicStatistics(12, 11, 10, 9));
        assertThat(reduce(
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4), SUBTRACT))
                .isEqualTo(new HiveBasicStatistics(10, 7, 4, 1));
    }

    @Test
    public void testUpdateTableStatistics()
    {
        testUpdateTableStatistics(ADD, createEmptyStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdateTableStatistics(SUBTRACT, createEmptyStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdateTableStatistics(ADD, createZeroStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdateTableStatistics(SUBTRACT, createZeroStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdateTableStatistics(ADD, createEmptyStatistics(), createZeroStatistics(), createEmptyStatistics());
        testUpdateTableStatistics(SUBTRACT, createEmptyStatistics(), createZeroStatistics(), createEmptyStatistics());
        testUpdateTableStatistics(
                ADD,
                new HiveBasicStatistics(1, 2, 3, 4),
                new HiveBasicStatistics(2, 3, 4, 5),
                new HiveBasicStatistics(3, 5, 7, 9));
        testUpdateTableStatistics(
                SUBTRACT,
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4),
                new HiveBasicStatistics(10, 7, 4, 1));
    }

    @Test
    public void testUpdatePartitionStatistics()
    {
        testUpdatePartitionStatistics(ADD, createEmptyStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdatePartitionStatistics(SUBTRACT, createEmptyStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdatePartitionStatistics(ADD, createZeroStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdatePartitionStatistics(SUBTRACT, createZeroStatistics(), createEmptyStatistics(), createEmptyStatistics());
        testUpdatePartitionStatistics(ADD, createEmptyStatistics(), createZeroStatistics(), createEmptyStatistics());
        testUpdatePartitionStatistics(SUBTRACT, createEmptyStatistics(), createZeroStatistics(), createEmptyStatistics());
        testUpdatePartitionStatistics(
                ADD,
                new HiveBasicStatistics(1, 2, 3, 4),
                new HiveBasicStatistics(2, 3, 4, 5),
                new HiveBasicStatistics(3, 5, 7, 9));
        testUpdatePartitionStatistics(
                SUBTRACT,
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4),
                new HiveBasicStatistics(10, 7, 4, 1));
    }

    private static void testUpdateTableStatistics(ReduceOperator operator, HiveBasicStatistics initial, HiveBasicStatistics update, HiveBasicStatistics expected)
    {
        Table initialTable = table(initial);
        Table updatedTable = updateStatistics(initialTable, update, operator);
        HiveBasicStatistics updatedStatistics = createFromPartitionParameters(updatedTable.getParameters());
        assertThat(updatedStatistics).isEqualTo(expected);
    }

    private static void testUpdatePartitionStatistics(ReduceOperator operator, HiveBasicStatistics initial, HiveBasicStatistics update, HiveBasicStatistics expected)
    {
        Partition initialPartition = partition(initial);
        Partition updatedPartition = updateStatistics(initialPartition, update, operator);
        HiveBasicStatistics updatedStatistics = createFromPartitionParameters(updatedPartition.getParameters());
        assertThat(updatedStatistics).isEqualTo(expected);
    }

    private static Table table(HiveBasicStatistics statistics)
    {
        return Table.builder(getPrestoTestTable("test_database"))
                .setParameters(statistics.toPartitionParameters())
                .build();
    }

    private static Partition partition(HiveBasicStatistics statistics)
    {
        return Partition.builder(getPrestoTestPartition("test_database", "test_table", ImmutableList.of("test_partition")))
                .setParameters(statistics.toPartitionParameters())
                .build();
    }
}
