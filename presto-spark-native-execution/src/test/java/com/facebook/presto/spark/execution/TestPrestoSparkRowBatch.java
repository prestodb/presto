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
package com.facebook.presto.spark.execution;

import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.PrestoSparkRowBatchBuilder;
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.RowIndex;
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.RowTupleSupplier;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.BYTES;
import static java.lang.Integer.MAX_VALUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkRowBatch
{
    private static final int REPLICATED_ROW_PARTITION_ID = -1;

    private static final int DEFAULT_TARGET_SIZE = 1024 * 1024;
    private static final int DEFAULT_EXPECTED_ROWS = 10000;
    private static final int NO_TARGET_ENTRY_SIZE_REQUIREMENT = 0;
    private static final int UNLIMITED_MAX_ENTRY_ROW_COUNT = MAX_VALUE;
    private static final int UNLIMITED_MAX_ENTRY_SIZE = MAX_VALUE;

    @Test
    public void testRoundTrip()
    {
        assertRoundTrip(ImmutableList.of());
        assertRoundTrip(ImmutableList.of(
                createRow(1, "row_data_1")));
        assertRoundTrip(ImmutableList.of(
                createRow(1, "")));
        assertRoundTrip(ImmutableList.of(
                createRow(1, ""),
                createRow(1, "")));
        assertRoundTrip(ImmutableList.of(
                createRow(1, ""),
                createRow(1, ""),
                createRow(1, "")));
        assertRoundTrip(ImmutableList.of(
                createRow(1, "row_data_1"),
                createRow(1, "row_data_2")));
        assertRoundTrip(ImmutableList.of(
                createRow(1, "row_data_1"),
                createRow(2, "row_data_2")));
        assertRoundTrip(ImmutableList.of(
                createRow(1, "row_data_1"),
                createRow(2, "row_data_2")));
        assertRoundTrip(IntStream.range(0, 4)
                .mapToObj(i -> createRow(i, "row_data_" + i))
                .collect(toImmutableList()));
        assertRoundTrip(IntStream.range(0, 5)
                .mapToObj(i -> createRow(i, "row_data"))
                .collect(toImmutableList()));
        assertRoundTrip(IntStream.range(0, 20)
                .mapToObj(i -> createRow(i, ""))
                .collect(toImmutableList()));
        assertRoundTrip(IntStream.range(0, 20)
                .mapToObj(i -> createRow(0, ""))
                .collect(toImmutableList()));
    }

    @Test
    public void testBuilderFull()
    {
        PrestoSparkRowBatchBuilder builder = PrestoSparkRowBatch.builder(
                10,
                5,
                10,
                NO_TARGET_ENTRY_SIZE_REQUIREMENT,
                UNLIMITED_MAX_ENTRY_SIZE,
                UNLIMITED_MAX_ENTRY_ROW_COUNT);
        assertFalse(builder.isFull());
        assertTrue(builder.isEmpty());
        addRow(builder, createRow(1, "12345"));
        assertTrue(builder.isFull());
        assertFalse(builder.isEmpty());
    }

    @Test
    public void testReplicatedRows()
    {
        assertRoundTrip(
                ImmutableList.of(
                        createReplicatedRow("replicated")),
                1,
                ImmutableList.of(
                        createRow(0, "replicated")));
        assertRoundTrip(
                ImmutableList.of(
                        createReplicatedRow("replicated")),
                2,
                ImmutableList.of(
                        createRow(0, "replicated"),
                        createRow(1, "replicated")));
        assertRoundTrip(
                ImmutableList.of(
                        createReplicatedRow("replicated")),
                3,
                ImmutableList.of(
                        createRow(0, "replicated"),
                        createRow(1, "replicated"),
                        createRow(2, "replicated")));
        assertRoundTrip(
                ImmutableList.of(
                        createReplicatedRow("replicated")),
                3,
                ImmutableList.of(
                        createRow(0, "replicated"),
                        createRow(1, "replicated"),
                        createRow(2, "replicated")));
        assertRoundTrip(
                ImmutableList.of(
                        createReplicatedRow("replicated"),
                        createRow(1, "non_replicated_1")),
                3,
                ImmutableList.of(
                        createRow(0, "replicated"),
                        createRow(1, "replicated"),
                        createRow(2, "replicated"),
                        createRow(1, "non_replicated_1")));
        assertRoundTrip(
                ImmutableList.of(
                        createRow(2, "non_replicated_22"),
                        createReplicatedRow("replicated")),
                3,
                ImmutableList.of(
                        createRow(0, "replicated"),
                        createRow(1, "replicated"),
                        createRow(2, "replicated"),
                        createRow(2, "non_replicated_22")));
        assertRoundTrip(
                ImmutableList.of(
                        createRow(1, "non_replicated_22"),
                        createReplicatedRow("replicated"),
                        createRow(0, "non_replicated_1")),
                2,
                ImmutableList.of(
                        createRow(0, "replicated"),
                        createRow(1, "replicated"),
                        createRow(1, "non_replicated_22"),
                        createRow(0, "non_replicated_1")));
        assertRoundTrip(
                ImmutableList.of(
                        createRow(1, "non_replicated_22"),
                        createReplicatedRow("replicated1"),
                        createReplicatedRow("replicated2"),
                        createRow(0, "non_replicated_1")),
                2,
                ImmutableList.of(
                        createRow(0, "replicated1"),
                        createRow(1, "replicated1"),
                        createRow(0, "replicated2"),
                        createRow(1, "replicated2"),
                        createRow(1, "non_replicated_22"),
                        createRow(0, "non_replicated_1")));
    }

    @Test
    public void testRowIndex()
    {
        assertRowIndex(new int[] {}, new int[][] {}, new int[] {});
        assertRowIndex(new int[] {}, new int[][] {new int[] {}, new int[] {}}, new int[] {});
        assertRowIndex(new int[] {0}, new int[][] {new int[] {0}, new int[] {}}, new int[] {});
        assertRowIndex(new int[] {1}, new int[][] {new int[] {}, new int[] {0}}, new int[] {});
        assertRowIndex(new int[] {0, 1}, new int[][] {new int[] {0}, new int[] {1}}, new int[] {});
        assertRowIndex(new int[] {0, 1, 1, 0, 0, 1}, new int[][] {new int[] {0, 3, 4}, new int[] {1, 2, 5}}, new int[] {});
        assertRowIndex(new int[] {0, 1, 1, 0, 0, 1}, new int[][] {new int[] {0, 3, 4}, new int[] {1, 2, 5}, new int[] {}}, new int[] {});
        assertRowIndex(new int[] {0, 1, 1, 2, 0, 2, 0, 1, 2}, new int[][] {new int[] {0, 4, 6}, new int[] {1, 2, 7}, new int[] {3, 5, 8}}, new int[] {});
        assertRowIndex(new int[] {1, 1, 1, 2, 1, 2, 1, 1, 2}, new int[][] {new int[] {}, new int[] {0, 1, 2, 4, 6, 7}, new int[] {3, 5, 8}}, new int[] {});
        assertRowIndex(new int[] {-1}, new int[][] {}, new int[] {0});
        assertRowIndex(new int[] {-1, -1}, new int[][] {}, new int[] {0, 1});
        assertRowIndex(new int[] {0, 1, -1}, new int[][] {new int[] {0}, new int[] {1}}, new int[] {2});
        assertRowIndex(new int[] {1, 1, 1, 2, -1, 1, 2, 1, 1, 2, -1}, new int[][] {new int[] {}, new int[] {0, 1, 2, 5, 7, 8}, new int[] {3, 6, 9}}, new int[] {4, 10});
    }

    @Test
    public void testMultiRowEntries()
    {
        Row row01 = createRow(0, "p0_1");
        Row row02 = createRow(0, "p0_11");
        Row row03 = createRow(0, "p0_111");
        Row row11 = createRow(1, "p1_1");
        Row row21 = createRow(2, "p2_1");

        List<Row> rows = ImmutableList.of(row01, row02, row03, row11, row21);

        assertEntries(
                rows,
                3,
                4,
                UNLIMITED_MAX_ENTRY_SIZE,
                UNLIMITED_MAX_ENTRY_ROW_COUNT,
                ImmutableList.of(
                        ImmutableList.of(row01),
                        ImmutableList.of(row02),
                        ImmutableList.of(row03),
                        ImmutableList.of(row11),
                        ImmutableList.of(row21)));
        assertEntries(
                rows,
                3,
                11,
                UNLIMITED_MAX_ENTRY_SIZE,
                UNLIMITED_MAX_ENTRY_ROW_COUNT,
                ImmutableList.of(
                        ImmutableList.of(row01, row02, row03),
                        ImmutableList.of(row11),
                        ImmutableList.of(row21)));
        assertEntries(
                rows,
                3,
                11,
                UNLIMITED_MAX_ENTRY_ROW_COUNT,
                2,
                ImmutableList.of(
                        ImmutableList.of(row01, row02),
                        ImmutableList.of(row03),
                        ImmutableList.of(row11),
                        ImmutableList.of(row21)));
        assertEntries(
                rows,
                3,
                11,
                18,
                UNLIMITED_MAX_ENTRY_ROW_COUNT,
                ImmutableList.of(
                        ImmutableList.of(row01, row02),
                        ImmutableList.of(row03),
                        ImmutableList.of(row11),
                        ImmutableList.of(row21)));
        assertEntries(
                rows,
                4,
                10,
                0,
                UNLIMITED_MAX_ENTRY_ROW_COUNT,
                ImmutableList.of(
                        ImmutableList.of(row01),
                        ImmutableList.of(row02),
                        ImmutableList.of(row03),
                        ImmutableList.of(row11),
                        ImmutableList.of(row21)));
        assertEntries(
                rows,
                3,
                10,
                UNLIMITED_MAX_ENTRY_SIZE,
                0,
                ImmutableList.of(
                        ImmutableList.of(row01),
                        ImmutableList.of(row02),
                        ImmutableList.of(row03),
                        ImmutableList.of(row11),
                        ImmutableList.of(row21)));
    }

    private static void assertRoundTrip(List<Row> rows)
    {
        // replicated rows are not allowed
        assertThat(rows).allMatch(row -> row.getPartition() >= 0);
        assertRoundTrip(rows, 20, rows);
    }

    private static void assertRoundTrip(List<Row> inputRows, int partitionCount, List<Row> expectedOutputRows)
    {
        PrestoSparkRowBatchBuilder singleRowEntryBuilder = PrestoSparkRowBatch.builder(
                partitionCount,
                DEFAULT_TARGET_SIZE,
                DEFAULT_EXPECTED_ROWS,
                0,
                UNLIMITED_MAX_ENTRY_SIZE,
                UNLIMITED_MAX_ENTRY_ROW_COUNT);
        assertRoundTrip(singleRowEntryBuilder, inputRows, partitionCount, expectedOutputRows);

        PrestoSparkRowBatchBuilder multiRowEntryBuilder = PrestoSparkRowBatch.builder(
                partitionCount,
                DEFAULT_TARGET_SIZE,
                DEFAULT_EXPECTED_ROWS,
                1024,
                UNLIMITED_MAX_ENTRY_SIZE,
                UNLIMITED_MAX_ENTRY_ROW_COUNT);
        assertRoundTrip(multiRowEntryBuilder, inputRows, partitionCount, expectedOutputRows);
    }

    private static void assertRoundTrip(PrestoSparkRowBatchBuilder builder, List<Row> inputRows, int partitionCount, List<Row> expectedOutputRows)
    {
        assertThat(inputRows).allMatch(row -> row.getPartition() < partitionCount);
        assertTrue(builder.isEmpty());
        for (Row row : inputRows) {
            addRow(builder, row);
        }
        assertFalse(builder.isFull());
        PrestoSparkRowBatch rowBatch = builder.build();
        assertContains(rowBatch, expectedOutputRows);
    }

    private static void assertEntries(
            List<Row> rows,
            int partitionCount,
            int targetEntrySize,
            int maxEntrySize,
            int maxRowsPerEntry,
            List<List<Row>> expectedEntries)
    {
        PrestoSparkRowBatchBuilder builder = PrestoSparkRowBatch.builder(
                partitionCount,
                DEFAULT_TARGET_SIZE,
                DEFAULT_EXPECTED_ROWS,
                targetEntrySize,
                maxEntrySize,
                maxRowsPerEntry);
        assertTrue(builder.isEmpty());
        for (Row row : rows) {
            addRow(builder, row);
        }
        assertFalse(builder.isFull());

        PrestoSparkRowBatch rowBatch = builder.build();

        List<List<Row>> actualEntries = getEntries(rowBatch);
        assertEquals(actualEntries, expectedEntries);
    }

    private static void addRow(PrestoSparkRowBatchBuilder builder, Row row)
    {
        SliceOutput output = builder.beginRowEntry();
        byte[] data = row.getData().getBytes(UTF_8);
        int bufferSize = data.length + BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.order(LITTLE_ENDIAN);
        buffer.putInt(data.length);
        buffer.put(data);
        output.writeBytes(buffer.array(), 0, bufferSize);
        if (row.isReplicated()) {
            builder.closeEntryForReplicatedRow();
        }
        else {
            builder.closeEntryForNonReplicatedRow(row.getPartition());
        }
    }

    private static void assertContains(PrestoSparkRowBatch rowBatch, List<Row> expected)
    {
        List<List<Row>> entries = getEntries(rowBatch);
        List<Row> rows = entries.stream()
                .flatMap(List::stream)
                .collect(toImmutableList());
        assertThat(rows)
                .containsExactlyInAnyOrder(expected.toArray(new Row[0]));
    }

    private static List<List<Row>> getEntries(PrestoSparkRowBatch rowBatch)
    {
        ImmutableList.Builder<List<Row>> entries = ImmutableList.builder();
        RowTupleSupplier rowTupleSupplier = rowBatch.createRowTupleSupplier();
        while (true) {
            Tuple2<MutablePartitionId, PrestoSparkMutableRow> next = rowTupleSupplier.getNext();
            if (next == null) {
                break;
            }
            ImmutableList.Builder<Row> entry = ImmutableList.builder();
            int partition = next._1.getPartition();
            PrestoSparkMutableRow mutableRow = next._2;
            ByteBuffer buffer = mutableRow.getBuffer();
            buffer.order(LITTLE_ENDIAN);
            short rowCount = buffer.getShort();
            assertEquals(mutableRow.getPositionCount(), rowCount);
            for (int i = 0; i < rowCount; i++) {
                entry.add(new Row(partition, readRowData(buffer)));
            }
            entries.add(entry.build());
        }
        return entries.build();
    }

    private static String readRowData(ByteBuffer buffer)
    {
        int size = buffer.getInt();
        String data = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), size, UTF_8);
        buffer.position(buffer.position() + size);
        return data;
    }

    private static Row createRow(int partition, String data)
    {
        return new Row(partition, data);
    }

    private static Row createReplicatedRow(String data)
    {
        return new Row(REPLICATED_ROW_PARTITION_ID, data);
    }

    private static class Row
    {
        private final int partition;
        private final String data;

        private Row(int partition, String data)
        {
            this.partition = partition;
            this.data = requireNonNull(data, "data is null");
        }

        public int getPartition()
        {
            return partition;
        }

        public String getData()
        {
            return data;
        }

        public boolean isReplicated()
        {
            return partition == REPLICATED_ROW_PARTITION_ID;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Row row = (Row) o;
            return partition == row.partition &&
                    Objects.equals(data, row.data);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partition, data);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("partition", partition)
                    .add("data", data)
                    .toString();
        }
    }

    private static void assertRowIndex(int[] partitions, int[][] expected, int[] expectedReplicated)
    {
        RowIndex rowIndex = RowIndex.create(partitions.length, expected.length, partitions);
        int[][] actual = new int[expected.length][];
        for (int partition = 0; partition < expected.length; partition++) {
            IntArrayList partitionRows = new IntArrayList();
            while (rowIndex.hasNextRow(partition)) {
                partitionRows.add(rowIndex.nextRow(partition));
            }
            actual[partition] = partitionRows.toIntArray();
        }
        assertThat(actual).isEqualTo(expected);

        IntArrayList replicatedRows = new IntArrayList();
        while (rowIndex.hasNextRow(REPLICATED_ROW_PARTITION_ID)) {
            replicatedRows.add(rowIndex.nextRow(REPLICATED_ROW_PARTITION_ID));
        }
        assertThat(replicatedRows.toIntArray()).isEqualTo(expectedReplicated);
    }
}
