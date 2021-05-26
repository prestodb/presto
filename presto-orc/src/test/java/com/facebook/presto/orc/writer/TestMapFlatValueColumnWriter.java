
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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createFlatMapValueColumnWriter;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createIntegerBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMapFlatValueColumnWriter
{
    @Test
    public void testColumnWriterCreation()
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);
        writer.beginRowGroup();
        writer.finishRowGroup();
        assertTrue(writer.getBufferedBytes() == 0, "Bytes not buffered");
    }

    @Test
    public void testWriteBlock()
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);

        Block[] blocks = createIntegerBlock();
        writer.beginRowGroup();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                Block singleValue = block.getRegion(position, 1);
                writer.writeBlock(singleValue);
            }
        }

        /* Verify ColumnStatistics */
        Map<Integer, ColumnStatistics> columnStats = writer.finishRowGroup();
        assertEquals(columnStats.size(), 1);
        assertEquals(columnStats.get(1).getNumberOfValues(), 10);
        writer.close();
    }

    @Test
    public void testGetIndexStreams()
            throws IOException
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);

        Block[] blocks = createIntegerBlock();
        writer.beginRowGroup();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                Block singleValue = block.getRegion(position, 1);
                writer.writeBlock(singleValue);
            }
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify Index Streams */
        List<StreamDataOutput> indexStreams = writer.getIndexStreams(Optional.empty());
        /* 11 index streams one for each unique value plus one for keyNode */
        assertEquals(indexStreams.size(), 1);
        assertEquals(indexStreams.get(0).getStream().getColumn(), 1);
        assertEquals(indexStreams.get(0).getStream().getSequence(), 1);
        assertEquals(indexStreams.get(0).getStream().getStreamKind(), Stream.StreamKind.ROW_INDEX);
    }

    @Test
    public void testGetColumnEncodings()
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);

        Block[] blocks = createIntegerBlock();
        writer.beginRowGroup();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                Block singleValue = block.getRegion(position, 1);
                writer.writeBlock(singleValue);
            }
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify the generated column encodings */
        Map<Integer, ColumnEncoding> columnEncodings = writer.getColumnEncodings();
        assertEquals(columnEncodings.size(), 1);
        assertEquals(columnEncodings.get(1).getColumnEncodingKind(), ColumnEncoding.ColumnEncodingKind.DIRECT);
        assertEquals(columnEncodings.get(1).getAdditionalSequenceEncodings().isPresent(), false);
    }

    @Test
    public void testGetStripeStatistics()
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);

        Block[] blocks = createIntegerBlock();
        writer.beginRowGroup();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                Block singleValue = block.getRegion(position, 1);
                writer.writeBlock(singleValue);
            }
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify the generated column stripe statistics */
        Map<Integer, ColumnStatistics> columnStripeStats = writer.getColumnStripeStatistics();
        assertEquals(columnStripeStats.size(), 1);
        assertEquals(columnStripeStats.get(1).getNumberOfValues(), 10);
    }

    @Test
    public void testGetDataStreams()
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);

        Block[] blocks = createIntegerBlock();
        writer.beginRowGroup();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                Block singleValue = block.getRegion(position, 1);
                writer.writeBlock(singleValue);
            }
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify the generated data streams */
        List<StreamDataOutput> dataStreams = writer.getDataStreams();
        assertEquals(dataStreams.size(), 2);
        assertEquals(dataStreams.get(0).getStream().getStreamKind(), Stream.StreamKind.IN_MAP);
        assertEquals(dataStreams.get(1).getStream().getStreamKind(), Stream.StreamKind.DATA);
        writer.reset();
    }

    @Test
    public void testWriteBlockWithMultipleRowGroups()
    {
        FlatMapValueColumnWriter writer = createFlatMapValueColumnWriter(INTEGER);

        Block[] blocks = createIntegerBlock();
        ImmutableList completedRowGroupPositions = ImmutableList.of(10, 10, 10);
        writer.finishRowGroups(completedRowGroupPositions);
        writer.beginRowGroup();
        int rowsWrittenInRowGroup = 0;
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                Block singleValue = block.getRegion(position, 1);
                writer.writeBlock(singleValue, rowsWrittenInRowGroup);
                /* one value written into the ValueColumnWriter and not in map entries */
                rowsWrittenInRowGroup += 10;
            }
        }

        Map<Integer, ColumnStatistics> columnStats = writer.finishRowGroup(rowsWrittenInRowGroup);
        assertEquals(columnStats.size(), 1);
        /* 10 actual values are written into the value writer */
        assertEquals(columnStats.get(1).getNumberOfValues(), 10);
        /* But there are 100 entries in IN MAP stream */
        assertEquals(writer.getRowGroupPosition(), rowsWrittenInRowGroup);
        writer.close();

        /* Verify the generated data streams */
        List<StreamDataOutput> dataStreams = writer.getDataStreams();
        assertEquals(dataStreams.size(), 2);
        assertEquals(dataStreams.get(0).getStream().getStreamKind(), Stream.StreamKind.IN_MAP);
        assertEquals(dataStreams.get(1).getStream().getStreamKind(), Stream.StreamKind.DATA);
        writer.reset();
    }
}
