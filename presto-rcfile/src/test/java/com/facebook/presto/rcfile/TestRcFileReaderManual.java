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
package com.facebook.presto.rcfile;

import com.facebook.presto.rcfile.binary.BinaryRcFileEncoding;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestRcFileReaderManual
{
    private static final Slice COLUMN_COUNT_METADATA_KEY = utf8Slice("hive.io.rcfile.column.number");
    private static final Slice RCFILE_MAGIC = utf8Slice("RCF");
    private static final int CURRENT_VERSION = 1;
    private static final long syncFirst = 0x1234_5678_9012_3456L;
    private static final long syncSecond = 0x7890_1234_5678_9012L;

    @Test
    public void testNoStartSync()
            throws Exception
    {
        SliceOutput output = new DynamicSliceOutput(10 * 1024);

        List<Segment> segments = ImmutableList.of(
                writeSegment(output, ImmutableList.of(ImmutableList.of(0, 2, 3, 4), ImmutableList.of(10, 12, 13))),
                writeSegment(output, ImmutableList.of(ImmutableList.of(20, 22), ImmutableList.of(30, 33), ImmutableList.of(40, 44))),
                writeSegment(output, ImmutableList.of(ImmutableList.of(100, 101, 102))));

        assertFileSegments(output.slice(), segments);
    }

    @Test
    public void testStartSync()
            throws Exception
    {
        SliceOutput output = new DynamicSliceOutput(10 * 1024);

        List<Segment> segments = ImmutableList.of(
                writeSegment(output, ImmutableList.of()),
                writeSegment(output, ImmutableList.of(ImmutableList.of(0, 2, 3, 4), ImmutableList.of(10, 12, 13))),
                writeSegment(output, ImmutableList.of(ImmutableList.of(20, 22), ImmutableList.of(30, 33), ImmutableList.of(40, 44))),
                writeSegment(output, ImmutableList.of(ImmutableList.of(100, 101, 102))));

        assertFileSegments(output.slice(), segments);
    }

    private static void assertFileSegments(Slice file, List<Segment> segments)
            throws IOException
    {
        // read whole file
        List<Integer> allValues = segments.stream()
                .map(Segment::getValues)
                .flatMap(List::stream)
                .collect(toList());
        assertEquals(allValues, readValues(file, 0, file.length()));

        for (Segment segment : segments) {
            // whole segment
            assertEquals(segment.getValues(), readValues(file, segment.getOffset(), segment.getLength()));
            // first byte of segment
            assertEquals(segment.getValues(), readValues(file, segment.getOffset(), 1));
            // straddle segment start
            assertEquals(segment.getValues(), readValues(file, segment.getOffset() - 1, 2));

            // regions entirely within the the segment
            assertEquals(ImmutableList.of(), readValues(file, segment.getOffset() + 1, 1));
            assertEquals(ImmutableList.of(), readValues(file, segment.getOffset() + 1, segment.getLength() - 1));

            for (int rowGroupOffset : segment.getRowGroupSegmentOffsets()) {
                // segment header to row group start
                assertEquals(segment.getValues(), readValues(file, segment.getOffset(), rowGroupOffset));
                assertEquals(segment.getValues(), readValues(file, segment.getOffset(), rowGroupOffset - 1));
                assertEquals(segment.getValues(), readValues(file, segment.getOffset(), rowGroupOffset + 1));

                // region from grow group start until end of file (row group offset is always inside of the segment since a
                // segment starts with a file header or sync sequence)
                assertEquals(ImmutableList.of(), readValues(file, segment.getOffset() + rowGroupOffset, segment.getLength() - rowGroupOffset));
            }
        }

        // all combinations of segments
        for (int startSegmentIndex = 0; startSegmentIndex < segments.size(); startSegmentIndex++) {
            Segment startSegment = segments.get(startSegmentIndex);
            for (int endSegmentIndex = startSegmentIndex; endSegmentIndex < segments.size(); endSegmentIndex++) {
                Segment endSegment = segments.get(endSegmentIndex);

                List<Integer> segmentsValues = segments.subList(startSegmentIndex, endSegmentIndex + 1).stream()
                        .map(Segment::getValues)
                        .flatMap(List::stream)
                        .collect(toList());

                assertEquals(segmentsValues, readValues(file, startSegment.getOffset(), endSegment.getOffset() + endSegment.getLength() - startSegment.getOffset()));
                assertEquals(segmentsValues, readValues(file, startSegment.getOffset(), endSegment.getOffset() + 1 - startSegment.getOffset()));
                assertEquals(segmentsValues, readValues(file, startSegment.getOffset() - 1, endSegment.getOffset() + 1 + endSegment.getLength() - startSegment.getOffset()));
                assertEquals(segmentsValues, readValues(file, startSegment.getOffset() - 1, endSegment.getOffset() + 1 + 1 - startSegment.getOffset()));
            }
        }
    }

    private static Segment writeSegment(SliceOutput output, List<List<Integer>> rowGroups)
    {
        int offset = output.size();

        // if we are at the beginning of the file write a file header, otherwise write a sync
        if (offset == 0) {
            writeFileHeader(output);
        }
        else {
            writeSync(output);
        }

        ImmutableList.Builder<Integer> rowGroupOffsets = ImmutableList.builder();
        for (List<Integer> rowGroup : rowGroups) {
            rowGroupOffsets.add(output.size() - offset);
            writeRowGroup(output, rowGroup);
        }
        int length = output.size() - offset;

        return new Segment(
                rowGroups.stream()
                        .flatMap(List::stream)
                        .collect(toList()),
                offset,
                length,
                rowGroupOffsets.build());
    }

    private static void writeFileHeader(SliceOutput output)
    {
        // write header
        output.writeBytes(RCFILE_MAGIC);
        output.writeByte(CURRENT_VERSION);

        // write codec information
        output.writeBoolean(false);

        // write metadata (which contains just the column count)
        output.writeInt(Integer.reverseBytes(1));
        output.writeByte(COLUMN_COUNT_METADATA_KEY.length());
        output.writeBytes(COLUMN_COUNT_METADATA_KEY);
        output.writeByte(1);
        output.writeByte('1');

        // write sync sequence
        output.writeLong(syncFirst);
        output.writeLong(syncSecond);
    }

    private static void writeSync(SliceOutput output)
    {
        output.writeInt(-1);
        output.writeLong(syncFirst);
        output.writeLong(syncSecond);
    }

    private static void writeRowGroup(SliceOutput output, List<Integer> shortValues)
    {
        // add arbitrary limit assure all lengths write as a simple single vint byte
        checkArgument(shortValues.size() < 32);

        // key section is 4 vint sizes followed by the column data
        int columnLengthsLength = shortValues.size();
        int keySectionLength = 4 + columnLengthsLength;

        int columnDataLength = shortValues.size() * 2;

        // write the sum of the uncompressed key length and compressed value length
        // this number is useless to the reader
        output.writeInt(Integer.reverseBytes(keySectionLength + columnDataLength));

        // key section: uncompressed size
        output.writeInt(Integer.reverseBytes(keySectionLength));
        // key section: compressed size
        output.writeInt(Integer.reverseBytes(keySectionLength));
        // key section: row count
        output.writeByte(shortValues.size());

        // key section: column data compressed size
        output.writeByte(columnDataLength);
        // key section: column data uncompressed size
        output.writeByte(columnDataLength);
        // key section: column lengths uncompressed size
        output.writeByte(columnLengthsLength);
        // key section: column lengths
        for (int ignored : shortValues) {
            output.write(2);
        }

        // value section: data
        for (int value : shortValues) {
            output.writeShort(Short.reverseBytes((short) value));
        }
    }

    private static List<Integer> readValues(Slice data, int offset, int length)
            throws IOException
    {
        // to simplify the testing:
        //     change negative offsets to 0
        //     truncate length so it is not off the end of the file

        if (offset < 0) {
            // adjust length to new offset
            length += offset;
            offset = 0;
        }
        if (offset + length > data.length()) {
            length = data.length() - offset;
        }

        RcFileReader reader = new RcFileReader(
                new SliceRcFileDataSource(data),
                new BinaryRcFileEncoding(),
                ImmutableMap.of(0, SMALLINT),
                codecName -> {
                    throw new UnsupportedOperationException();
                },
                offset,
                length,
                new DataSize(1, MEGABYTE));

        ImmutableList.Builder<Integer> values = ImmutableList.builder();
        while (reader.advance() >= 0) {
            Block block = reader.readBlock(0);
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add((int) SMALLINT.getLong(block, position));
            }
        }

        return values.build();
    }

    private static class Segment
    {
        private final List<Integer> values;
        private final int offset;
        private final int length;
        private final List<Integer> rowGroupSegmentOffsets;

        public Segment(List<Integer> values, int offset, int length, List<Integer> rowGroupSegmentOffsets)
        {
            this.values = ImmutableList.copyOf(values);
            this.offset = offset;
            this.length = length;
            this.rowGroupSegmentOffsets = ImmutableList.copyOf(rowGroupSegmentOffsets);
        }

        public List<Integer> getValues()
        {
            return values;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getLength()
        {
            return length;
        }

        public List<Integer> getRowGroupSegmentOffsets()
        {
            return rowGroupSegmentOffsets;
        }
    }

    private static class SliceRcFileDataSource
            implements RcFileDataSource
    {
        private final Slice data;

        public SliceRcFileDataSource(Slice data)
        {
            this.data = data;
        }

        @Override
        public long getReadBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getSize()
        {
            return data.length();
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            data.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
        }

        @Override
        public void close()
        {
        }
    }
}
