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

import com.facebook.presto.rcfile.RcFileCompressor.CompressedSliceOutput;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.writeLengthPrefixedString;
import static com.facebook.presto.rcfile.RcFileDecoderUtils.writeVInt;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class RcFileWriter
        implements Closeable
{
    private static final Slice RCFILE_MAGIC = utf8Slice("RCF");
    private static final int CURRENT_VERSION = 1;
    private static final String COLUMN_COUNT_METADATA_KEY = "hive.io.rcfile.column.number";
    private static final DataSize DEFAULT_TARGET_MIN_ROW_GROUP_SIZE = new DataSize(4, MEGABYTE);
    private static final DataSize DEFAULT_TARGET_MAX_ROW_GROUP_SIZE = new DataSize(8, MEGABYTE);
    private static final DataSize MIN_BUFFER_SIZE = new DataSize(4, KILOBYTE);
    private static final DataSize MAX_BUFFER_SIZE = new DataSize(1, MEGABYTE);

    static final String PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY = "presto.writer.version";
    static final String PRESTO_RCFILE_WRITER_VERSION;
    static {
        String version = RcFileWriter.class.getPackage().getImplementationVersion();
        PRESTO_RCFILE_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final SliceOutput output;

    private final long syncFirst = ThreadLocalRandom.current().nextLong();
    private final long syncSecond = ThreadLocalRandom.current().nextLong();

    private CompressedSliceOutput keySectionOutput;
    private final ColumnEncoder[] columnEncoders;

    private final int targetMinRowGroupSize;
    private final int targetMaxRowGroupSize;

    private int bufferedSize;
    private int bufferedRows;

    private long totalRowCount;

    public RcFileWriter(
            SliceOutput output,
            List<Type> types,
            RcFileEncoding encoding,
            Optional<String> codecName,
            RcFileCodecFactory codecFactory,
            Map<String, String> metadata)
            throws IOException
    {
        this(
                output,
                types,
                encoding,
                codecName,
                codecFactory,
                metadata,
                DEFAULT_TARGET_MIN_ROW_GROUP_SIZE,
                DEFAULT_TARGET_MAX_ROW_GROUP_SIZE);
    }

    public RcFileWriter(
            SliceOutput output,
            List<Type> types,
            RcFileEncoding encoding,
            Optional<String> codecName,
            RcFileCodecFactory codecFactory,
            Map<String, String> metadata,
            DataSize targetMinRowGroupSize,
            DataSize targetMaxRowGroupSize)
            throws IOException
    {
        requireNonNull(output, "output is null");
        requireNonNull(types, "types is null");
        checkArgument(!types.isEmpty(), "types is empty");
        requireNonNull(encoding, "encoding is null");
        requireNonNull(codecName, "codecName is null");
        requireNonNull(codecFactory, "codecFactory is null");
        requireNonNull(metadata, "metadata is null");
        checkArgument(!metadata.containsKey(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY), "Cannot set property %s", PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY);
        checkArgument(!metadata.containsKey(COLUMN_COUNT_METADATA_KEY), "Cannot set property %s", COLUMN_COUNT_METADATA_KEY);
        requireNonNull(targetMinRowGroupSize, "targetMinRowGroupSize is null");
        requireNonNull(targetMaxRowGroupSize, "targetMaxRowGroupSize is null");
        checkArgument(targetMinRowGroupSize.compareTo(targetMaxRowGroupSize) <= 0, "targetMinRowGroupSize must be less than or equal to targetMaxRowGroupSize");

        this.output = output;

        // write header
        output.writeBytes(RCFILE_MAGIC);
        output.writeByte(CURRENT_VERSION);

        // write codec information
        output.writeBoolean(codecName.isPresent());
        codecName.ifPresent(name -> writeLengthPrefixedString(output, utf8Slice(name)));

        // write metadata
        output.writeInt(Integer.reverseBytes(metadata.size() + 2));
        writeLengthPrefixedString(output, utf8Slice(COLUMN_COUNT_METADATA_KEY));
        writeLengthPrefixedString(output, utf8Slice(Integer.toString(types.size())));
        writeLengthPrefixedString(output, utf8Slice(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY));
        writeLengthPrefixedString(output, utf8Slice(PRESTO_RCFILE_WRITER_VERSION));
        for (Entry<String, String> entry : metadata.entrySet()) {
            writeLengthPrefixedString(output, utf8Slice(entry.getKey()));
            writeLengthPrefixedString(output, utf8Slice(entry.getValue()));
        }

        // write sync sequence
        output.writeLong(syncFirst);
        output.writeLong(syncSecond);

        // initialize columns
        RcFileCompressor compressor = codecName.map(codecFactory::createCompressor).orElse(new NoneCompressor());
        keySectionOutput = compressor.createCompressedSliceOutput((int) MIN_BUFFER_SIZE.toBytes(), (int) MAX_BUFFER_SIZE.toBytes());
        keySectionOutput.close(); // output is recycled on first use which requires output to be closed
        columnEncoders = new ColumnEncoder[types.size()];
        for (int columnIndex = 0; columnIndex < types.size(); columnIndex++) {
            Type type = types.get(columnIndex);
            ColumnEncoding columnEncoding = encoding.getEncoding(type);
            columnEncoders[columnIndex] = new ColumnEncoder(columnEncoding, compressor);
        }
        this.targetMinRowGroupSize = toIntExact(targetMinRowGroupSize.toBytes());
        this.targetMaxRowGroupSize = toIntExact(targetMaxRowGroupSize.toBytes());
    }

    @Override
    public void close()
            throws IOException
    {
        writeRowGroup();
        output.close();
        keySectionOutput.destroy();
        for (ColumnEncoder columnEncoder : columnEncoders) {
            columnEncoder.destroy();
        }
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSize = 0;
        retainedSize += output.getRetainedSize();
        retainedSize += keySectionOutput.getRetainedSize();
        for (ColumnEncoder columnEncoder : columnEncoders) {
            retainedSize += columnEncoder.getRetainedSizeInBytes();
        }
        return retainedSize;
    }

    public void write(Page page)
            throws IOException
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        long pageSize = page.getSizeInBytes();
        if (pageSize <= targetMaxRowGroupSize || page.getPositionCount() == 1) {
            bufferPage(page);
            return;
        }

        // recursively split page until it is less than the max row group size
        int positionCount = page.getPositionCount();
        int half = positionCount / 2;
        write(page.getRegion(0, half));
        write(page.getRegion(half, positionCount - half));
    }

    private void bufferPage(Page page)
            throws IOException
    {
        bufferedRows += page.getPositionCount();

        bufferedSize = 0;
        Block[] blocks = page.getBlocks();
        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            columnEncoders[i].writeBlock(block);
            bufferedSize += columnEncoders[i].getBufferedSize();
        }

        if (bufferedSize >= targetMinRowGroupSize) {
            writeRowGroup();
        }
    }

    private void writeRowGroup()
            throws IOException
    {
        if (bufferedRows == 0) {
            return;
        }

        // write sync sequence for every row group except for the first row group
        if (totalRowCount != 0) {
            output.writeInt(-1);
            output.writeLong(syncFirst);
            output.writeLong(syncSecond);
        }

        // flush and compress each column
        for (ColumnEncoder columnEncoder : columnEncoders) {
            columnEncoder.closeColumn();
        }

        // build key section
        keySectionOutput = keySectionOutput.createRecycledCompressedSliceOutput();
        writeVInt(keySectionOutput, bufferedRows);

        int valueLength = 0;
        for (ColumnEncoder columnEncoder : columnEncoders) {
            valueLength += columnEncoder.getCompressedSize();
            writeVInt(keySectionOutput, columnEncoder.getCompressedSize());
            writeVInt(keySectionOutput, columnEncoder.getUncompressedSize());

            Slice lengthData = columnEncoder.getLengthData();
            writeVInt(keySectionOutput, lengthData.length());
            keySectionOutput.writeBytes(lengthData);
        }
        keySectionOutput.close();

        // write the sum of the uncompressed key length and compressed value length
        // this number is useless to the reader
        output.writeInt(Integer.reverseBytes(keySectionOutput.size() + valueLength));

        // write key section
        output.writeInt(Integer.reverseBytes(keySectionOutput.size()));
        output.writeInt(Integer.reverseBytes(keySectionOutput.getCompressedSize()));
        for (Slice slice : keySectionOutput.getCompressedSlices()) {
            output.writeBytes(slice);
        }

        // write value section
        for (ColumnEncoder columnEncoder : columnEncoders) {
            List<Slice> slices = columnEncoder.getCompressedData();
            for (Slice slice : slices) {
                output.writeBytes(slice);
            }
            columnEncoder.reset();
        }

        totalRowCount += bufferedRows;
        bufferedSize = 0;
        bufferedRows = 0;
    }

    private static class ColumnEncoder
    {
        private final ColumnEncoding columnEncoding;

        private ColumnEncodeOutput encodeOutput;

        private final SliceOutput lengthOutput = new DynamicSliceOutput(512);

        private CompressedSliceOutput output;

        private boolean columnClosed;

        public ColumnEncoder(ColumnEncoding columnEncoding, RcFileCompressor compressor)
                throws IOException
        {
            this.columnEncoding = columnEncoding;
            this.output = compressor.createCompressedSliceOutput((int) MIN_BUFFER_SIZE.toBytes(), (int) MAX_BUFFER_SIZE.toBytes());
            this.encodeOutput = new ColumnEncodeOutput(lengthOutput, output);
        }

        private void writeBlock(Block block)
                throws IOException
        {
            checkArgument(!columnClosed, "Column is closed");
            columnEncoding.encodeColumn(block, output, encodeOutput);
        }

        public void closeColumn()
                throws IOException
        {
            checkArgument(!columnClosed, "Column is not open");

            encodeOutput.flush();
            output.close();

            columnClosed = true;
        }

        public int getBufferedSize()
        {
            return lengthOutput.size() + output.size();
        }

        public Slice getLengthData()
        {
            checkArgument(columnClosed, "Column is open");
            return lengthOutput.slice();
        }

        public int getUncompressedSize()
        {
            checkArgument(columnClosed, "Column is open");
            return output.size();
        }

        public int getCompressedSize()
        {
            checkArgument(columnClosed, "Column is open");
            return output.getCompressedSize();
        }

        public List<Slice> getCompressedData()
        {
            checkArgument(columnClosed, "Column is open");
            return output.getCompressedSlices();
        }

        public void reset()
                throws IOException
        {
            checkArgument(columnClosed, "Column is open");
            lengthOutput.reset();

            output = output.createRecycledCompressedSliceOutput();
            encodeOutput = new ColumnEncodeOutput(lengthOutput, output);

            columnClosed = false;
        }

        public void destroy()
        {
            output.destroy();
        }

        public long getRetainedSizeInBytes()
        {
            return lengthOutput.getRetainedSize() + output.getRetainedSize();
        }

        private static class ColumnEncodeOutput
                implements EncodeOutput
        {
            private final SliceOutput lengthOutput;
            private final SliceOutput valueOutput;
            private int previousOffset;
            private int previousLength;
            private int runLength;

            public ColumnEncodeOutput(SliceOutput lengthOutput, SliceOutput valueOutput)
            {
                this.lengthOutput = lengthOutput;
                this.valueOutput = valueOutput;
                this.previousOffset = valueOutput.size();
                this.previousLength = -1;
            }

            @Override
            public void closeEntry()
            {
                int offset = valueOutput.size();
                int length = offset - previousOffset;
                previousOffset = offset;

                if (length == previousLength) {
                    runLength++;
                }
                else {
                    if (runLength > 0) {
                        int value = ~runLength;
                        writeVInt(lengthOutput, value);
                    }
                    writeVInt(lengthOutput, length);
                    previousLength = length;
                    runLength = 0;
                }
            }

            private void flush()
            {
                if (runLength > 0) {
                    int value = ~runLength;
                    writeVInt(lengthOutput, value);
                }
                previousLength = -1;
                runLength = 0;
            }
        }
    }
}
