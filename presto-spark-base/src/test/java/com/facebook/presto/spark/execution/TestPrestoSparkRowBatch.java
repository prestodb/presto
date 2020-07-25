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
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.RowTupleSupplier;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkRowBatch
{
    @Test
    public void testRoundTrip()
    {
        assertRoundTrip(ImmutableList.of());
        assertRoundTrip(ImmutableList.of(createTuple(1, "row_data_1")));
        assertRoundTrip(ImmutableList.of(createTuple(1, "")));
        assertRoundTrip(ImmutableList.of(createTuple(1, ""), createTuple(1, "")));
        assertRoundTrip(ImmutableList.of(createTuple(1, ""), createTuple(1, ""), createTuple(1, "")));
        assertRoundTrip(ImmutableList.of(createTuple(1, "row_data_1"), createTuple(1, "row_data_2")));
        assertRoundTrip(ImmutableList.of(createTuple(1, "row_data_1"), createTuple(2, "row_data_2")));
        assertRoundTrip(ImmutableList.of(createTuple(1, "row_data_1"), createTuple(2, "row_data_2")));
        assertRoundTrip(IntStream.range(0, 4)
                .mapToObj(i -> createTuple(i, "row_data_" + i))
                .collect(toImmutableList()));
        assertRoundTrip(IntStream.range(0, 5)
                .mapToObj(i -> createTuple(i, "row_data"))
                .collect(toImmutableList()));
        assertRoundTrip(IntStream.range(0, 20)
                .mapToObj(i -> createTuple(i, ""))
                .collect(toImmutableList()));
        assertRoundTrip(IntStream.range(0, 20)
                .mapToObj(i -> createTuple(0, ""))
                .collect(toImmutableList()));
    }

    @Test
    public void testBuilderFull()
    {
        PrestoSparkRowBatchBuilder builder = PrestoSparkRowBatch.builder(10, 5, 10);
        assertFalse(builder.isFull());
        assertTrue(builder.isEmpty());
        addRow(createTuple(1, "12345"), builder);
        assertTrue(builder.isFull());
        assertFalse(builder.isEmpty());
    }

    @Test
    public void testReplicatedRows()
    {
        PrestoSparkRowBatchBuilder builder = PrestoSparkRowBatch.builder(1);
        addReplicatedRow(createRow("replicated"), builder);
        assertContains(builder.build(), ImmutableList.of(createTuple(0, "replicated")));

        builder = PrestoSparkRowBatch.builder(2);
        addReplicatedRow(createRow("replicated"), builder);
        assertContains(builder.build(), ImmutableList.of(createTuple(1, "replicated"), createTuple(0, "replicated")));

        builder = PrestoSparkRowBatch.builder(3);
        addReplicatedRow(createRow("replicated"), builder);
        assertContains(builder.build(), ImmutableList.of(createTuple(2, "replicated"), createTuple(1, "replicated"), createTuple(0, "replicated")));

        builder = PrestoSparkRowBatch.builder(2);
        addReplicatedRow(createRow("replicated"), builder);
        addRow(createTuple(101, "non_replicated_1"), builder);
        assertContains(builder.build(), ImmutableList.of(
                createTuple(1, "replicated"),
                createTuple(0, "replicated"),
                createTuple(101, "non_replicated_1")));

        builder = PrestoSparkRowBatch.builder(2);
        addRow(createTuple(202, "non_replicated_22"), builder);
        addReplicatedRow(createRow("replicated"), builder);
        assertContains(builder.build(), ImmutableList.of(
                createTuple(202, "non_replicated_22"),
                createTuple(1, "replicated"),
                createTuple(0, "replicated")));

        builder = PrestoSparkRowBatch.builder(2);
        addRow(createTuple(202, "non_replicated_22"), builder);
        addReplicatedRow(createRow("replicated"), builder);
        addRow(createTuple(101, "non_replicated_1"), builder);
        assertContains(builder.build(), ImmutableList.of(
                createTuple(202, "non_replicated_22"),
                createTuple(1, "replicated"),
                createTuple(0, "replicated"),
                createTuple(101, "non_replicated_1")));

        builder = PrestoSparkRowBatch.builder(2);
        addRow(createTuple(202, "non_replicated_22"), builder);
        addReplicatedRow(createRow("replicated"), builder);
        addReplicatedRow(createRow("replicated"), builder);
        addRow(createTuple(101, "non_replicated_1"), builder);
        assertContains(builder.build(), ImmutableList.of(
                createTuple(202, "non_replicated_22"),
                createTuple(1, "replicated"),
                createTuple(0, "replicated"),
                createTuple(1, "replicated"),
                createTuple(0, "replicated"),
                createTuple(101, "non_replicated_1")));
    }

    private static void assertRoundTrip(List<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> rows)
    {
        PrestoSparkRowBatchBuilder builder = PrestoSparkRowBatch.builder(10);
        assertTrue(builder.isEmpty());
        rows.forEach(row -> addRow(row, builder));
        assertFalse(builder.isFull());
        PrestoSparkRowBatch rowBatch = builder.build();
        assertContains(rowBatch, rows);
    }

    private static void addRow(Tuple2<MutablePartitionId, PrestoSparkMutableRow> row, PrestoSparkRowBatchBuilder builder)
    {
        SliceOutput output = builder.beginRowEntry();
        ByteBuffer buffer = row._2.getBuffer();
        output.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        builder.closeEntryForNonReplicatedRow(row._1.getPartition());
    }

    private static void addReplicatedRow(PrestoSparkMutableRow row, PrestoSparkRowBatchBuilder builder)
    {
        SliceOutput output = builder.beginRowEntry();
        ByteBuffer buffer = row.getBuffer();
        output.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        builder.closeEntryForReplicatedRow();
    }

    private static Tuple2<MutablePartitionId, PrestoSparkMutableRow> createTuple(int partition, String data)
    {
        MutablePartitionId mutablePartitionId = new MutablePartitionId();
        mutablePartitionId.setPartition(partition);
        return new Tuple2<>(mutablePartitionId, createRow(data));
    }

    private static PrestoSparkMutableRow createRow(String data)
    {
        PrestoSparkMutableRow mutableRow = new PrestoSparkMutableRow();
        mutableRow.setBuffer(ByteBuffer.wrap(data.getBytes(UTF_8)));
        return mutableRow;
    }

    private static void assertContains(PrestoSparkRowBatch rowBatch, List<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> expected)
    {
        RowTupleSupplier rowTupleSupplier = rowBatch.createRowTupleSupplier();
        ImmutableList.Builder<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> actual = ImmutableList.builder();
        while (true) {
            Tuple2<MutablePartitionId, PrestoSparkMutableRow> next = rowTupleSupplier.getNext();
            if (next == null) {
                break;
            }
            actual.add(copy(next));
        }
        assertTupleEquals(actual.build(), expected);
    }

    private static void assertTupleEquals(
            List<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> actual,
            List<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> expected)
    {
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTupleEquals(actual.get(i), expected.get(i));
        }
    }

    private static void assertTupleEquals(Tuple2<MutablePartitionId, PrestoSparkMutableRow> actual, Tuple2<MutablePartitionId, PrestoSparkMutableRow> expected)
    {
        assertEquals(actual._1.getPartition(), expected._1.getPartition());
        ByteBuffer actualBuffer = actual._2.getBuffer();
        String actualData = new String(actualBuffer.array(), actualBuffer.arrayOffset() + actualBuffer.position(), actualBuffer.remaining());
        ByteBuffer expectedBuffer = expected._2.getBuffer();
        String expectedData = new String(expectedBuffer.array(), expectedBuffer.arrayOffset() + expectedBuffer.position(), expectedBuffer.remaining());
        assertEquals(actualData, expectedData);
    }

    private static Tuple2<MutablePartitionId, PrestoSparkMutableRow> copy(Tuple2<MutablePartitionId, PrestoSparkMutableRow> tuple)
    {
        return new Tuple2<>(copy(tuple._1), copy(tuple._2));
    }

    private static PrestoSparkMutableRow copy(PrestoSparkMutableRow mutableRow)
    {
        PrestoSparkMutableRow copy = new PrestoSparkMutableRow();
        ByteBuffer bufferCopy = ByteBuffer.allocate(mutableRow.getBuffer().remaining());
        bufferCopy.put(mutableRow.getBuffer());
        bufferCopy.position(0);
        copy.setBuffer(bufferCopy);
        return copy;
    }

    private static MutablePartitionId copy(MutablePartitionId mutablePartitionId)
    {
        MutablePartitionId copy = new MutablePartitionId();
        copy.setPartition(mutablePartitionId.getPartition());
        return copy;
    }
}
