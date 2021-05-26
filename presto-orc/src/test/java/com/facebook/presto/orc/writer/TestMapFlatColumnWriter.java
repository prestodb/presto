
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
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createFlatMapColumnWriter;
import static com.facebook.presto.orc.writer.TestMapFlatUtils.createMapBlockWithIntegerData;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMapFlatColumnWriter
{
    private static final int STRIPE_MAX_ROWS = 15_000;

    @Test
    public void testFlatMapColumnWriterCreation()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);
        writer.beginRowGroup();
        writer.finishRowGroup();
        assertTrue(writer.getBufferedBytes() == 0, "Bytes not buffered");
    }

    @Test
    public void testFlatMapWriteBlock()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        /* Verify ColumnStatistics */
        Map<Integer, ColumnStatistics> columnStats = writer.finishRowGroup();
        assertEquals(columnStats.size(), 3);
        assertEquals(columnStats.get(1).getNumberOfValues(), 10);
        assertEquals(columnStats.get(2).getNumberOfValues(), 100);
        assertEquals(columnStats.get(3).getNumberOfValues(), 100);

        writer.close();
    }

    @Test
    public void testFlatMapIndexStreams()
            throws IOException
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify Index Streams */
        List<StreamDataOutput> indexStreams = writer.getIndexStreams(Optional.empty());
        /* 11 index streams one for each unique value plus one for keyNode */
        assertEquals(indexStreams.size(), 11);
        assertEquals(indexStreams.get(0).getStream().getColumn(), 1);
        assertEquals(indexStreams.get(0).getStream().getStreamKind(), Stream.StreamKind.ROW_INDEX);
        for (int streamNum = 1; streamNum < 11; streamNum++) {
            assertEquals(indexStreams.get(streamNum).getStream().getColumn(), 3);
            assertEquals(indexStreams.get(streamNum).getStream().getStreamKind(), Stream.StreamKind.ROW_INDEX);
        }
    }

    @Test
    public void testFlatMapColumnEncodings()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();

        /* Verify the generated column encodings */
        Map<Integer, ColumnEncoding> columnEncodings = writer.getColumnEncodings();
        assertEquals(columnEncodings.size(), 2);
        assertEquals(columnEncodings.get(1).getColumnEncodingKind(), ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT);
        assertEquals(columnEncodings.get(3).getColumnEncodingKind(), ColumnEncoding.ColumnEncodingKind.DIRECT);
        assertEquals(columnEncodings.get(1).getAdditionalSequenceEncodings().isPresent(), false);
        assertEquals(columnEncodings.get(3).getAdditionalSequenceEncodings().get().size(), 10);
    }

    @Test
    public void testFlatMapStripeStatistics()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();
        /* Verify the generated column stripe statistics */
        Map<Integer, ColumnStatistics> columnStripeStats = writer.getColumnStripeStatistics();
        assertEquals(columnStripeStats.size(), 3);
        assertEquals(columnStripeStats.size(), 3);
        assertEquals(columnStripeStats.get(1).getNumberOfValues(), 10);
        assertEquals(columnStripeStats.get(2).getNumberOfValues(), 100);
        assertEquals(columnStripeStats.get(3).getNumberOfValues(), 100);
    }

    @Test
    public void testFlatMapDataStreams()
    {
        FlatMapColumnWriter writer = createFlatMapColumnWriter(INTEGER, INTEGER);

        Block[] blocks = createMapBlockWithIntegerData();
        writer.beginRowGroup();
        for (Block block : blocks) {
            writer.writeBlock(block);
        }

        writer.finishRowGroup();
        writer.close();
        /* Verify the generated data streams */
        List<StreamDataOutput> dataStreams = writer.getDataStreams();
        assertEquals(dataStreams.size(), 20);
        for (int streamNum = 0; streamNum < 10; streamNum++) {
            assertEquals(dataStreams.get(streamNum * 2).getStream().getStreamKind(), Stream.StreamKind.IN_MAP);
            assertEquals(dataStreams.get(streamNum * 2 + 1).getStream().getStreamKind(), Stream.StreamKind.DATA);
        }
        writer.reset();
    }
}
