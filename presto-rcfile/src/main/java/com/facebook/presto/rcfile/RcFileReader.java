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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.ChunkedSliceInput;
import io.airlift.slice.ChunkedSliceInput.BufferReference;
import io.airlift.slice.ChunkedSliceInput.SliceLoader;
import io.airlift.slice.RuntimeIOException;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.findFirstSyncPosition;
import static com.facebook.presto.rcfile.RcFileDecoderUtils.readVInt;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.ByteStreams.skipFully;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class RcFileReader
        implements Closeable
{
    private static final Slice RCFILE_MAGIC = Slices.utf8Slice("RCF");

    // These numbers were chosen arbitrarily, and are only to prevent corrupt files from causing OOMs
    private static final int MAX_METADATA_ENTRIES = 500_000;
    private static final int MAX_COLUMN_COUNT = 500_000;
    private static final int MAX_METADATA_STRING_LENGTH = 1024 * 1024;

    private static final int FIRST_VERSION = 0;
    private static final int CURRENT_VERSION = 1;

    // First version of RCFile use Sequence file magic with version 6
    private static final Slice SEQUENCE_FILE_MAGIC = Slices.utf8Slice("SEQ");
    private static final byte SEQUENCE_FILE_VERSION = 6;
    private static final Slice RCFILE_KEY_BUFFER_NAME = Slices.utf8Slice("org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer");
    private static final Slice RCFILE_VALUE_BUFFER_NAME = Slices.utf8Slice("org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer");

    private static final String COLUMN_COUNT_METADATA_KEY = "hive.io.rcfile.column.number";

    private final RcFileDataSource dataSource;
    private final Set<Integer> readColumns;
    private final ChunkedSliceInput input;
    private final long length;

    private final byte version;

    private final RcFileDecompressor decompressor;

    private final Map<String, String> metadata;
    private final int columnCount;

    private final long syncFirst;
    private final long syncSecond;

    private final Column[] columns;
    private final long end;

    private long rowsRead;

    private int rowGroupRowCount;
    private int rowGroupPosition;

    private int currentChunkRowCount;

    private Slice compressedHeaderBuffer = Slices.EMPTY_SLICE;
    private Slice headerBuffer = Slices.EMPTY_SLICE;

    private boolean closed;

    public RcFileReader(
            RcFileDataSource dataSource,
            List<Type> types,
            RcFileEncoding encoding,
            Set<Integer> readColumns,
            RcFileCodecFactory codecFactory,
            long offset,
            long length,
            DataSize bufferSize)
            throws IOException
    {
        this.dataSource = requireNonNull(dataSource, "rcFileDataSource is null");
        this.readColumns = ImmutableSet.copyOf(requireNonNull(readColumns, "readColumns is null"));
        this.input = new ChunkedSliceInput(new DataSourceSliceLoader(dataSource), Ints.checkedCast(bufferSize.toBytes()));

        checkArgument(offset >= 0, "offset is negative");
        checkArgument(offset < dataSource.getSize(), "offset is greater than data size");
        checkArgument(length >= 1, "length must be at least 1");
        this.length = length;
        this.end = offset + length;
        checkArgument(end <= dataSource.getSize(), "offset plus length is greater than data size");

        // read header
        Slice magic = input.readSlice(RCFILE_MAGIC.length());
        boolean compressed;
        if (RCFILE_MAGIC.equals(magic)) {
            version = input.readByte();
            if (version > CURRENT_VERSION) {
                throw corrupt("RCFile version %s not supported: %s", version, dataSource);
            }

            compressed = input.readBoolean();
        }
        else if (SEQUENCE_FILE_MAGIC.equals(magic)) {
            // first version of RCFile used magic SEQ with version 6
            byte sequenceFileVersion = input.readByte();
            if (sequenceFileVersion != SEQUENCE_FILE_VERSION) {
                throw corrupt("File %s is a SequenceFile not an RCFile", dataSource);
            }

            // this is the first version of RCFile
            this.version = FIRST_VERSION;

            Slice keyClassName = readLengthPrefixedString(input);
            Slice valueClassName = readLengthPrefixedString(input);
            if (!RCFILE_KEY_BUFFER_NAME.equals(keyClassName) || !RCFILE_VALUE_BUFFER_NAME.equals(valueClassName)) {
                throw corrupt("File %s is a SequenceFile not an RCFile", dataSource);
            }

            compressed = input.readBoolean();

            // RC file is never block compressed
            if (input.readBoolean()) {
                throw corrupt("File %s is a SequenceFile not an RCFile", dataSource);
            }
        }
        else {
            throw corrupt("File %s is not an RCFile", dataSource);
        }

        // setup the compression codec
        if (compressed) {
            Slice codecClassName = readLengthPrefixedString(input);
            this.decompressor = codecFactory.createDecompressor(codecClassName.toStringUtf8());
        }
        else {
            this.decompressor = null;
        }

        // read metadata
        int metadataEntries = Integer.reverseBytes(input.readInt());
        if (metadataEntries < 0) {
            throw corrupt("Invalid metadata entry count %s in RCFile %s", metadataEntries, dataSource);
        }
        if (metadataEntries > MAX_METADATA_ENTRIES) {
            throw corrupt("Too many metadata entries (%s) in RCFile %s", metadataEntries, dataSource);
        }
        ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
        for (int i = 0; i < metadataEntries; i++) {
            metadataBuilder.put(readLengthPrefixedString(input).toStringUtf8(), readLengthPrefixedString(input).toStringUtf8());
        }
        metadata = metadataBuilder.build();

        // get column count from metadata
        String columnCountString = metadata.get(COLUMN_COUNT_METADATA_KEY);
        try {
            columnCount = Integer.parseInt(columnCountString);
        }
        catch (NumberFormatException e) {
            throw corrupt("Invalid column count %s in RCFile %s", columnCountString, dataSource);
        }

        // initialize columns
        if (columnCount > MAX_COLUMN_COUNT) {
            throw corrupt("Too many columns (%s) in RCFile %s", columnCountString, dataSource);
        }
        columns = new Column[columnCount];
        for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
            ColumnEncoding columnEncoding = encoding.getEncoding(types.get(columnIndex));
            columns[columnIndex] = new Column(columnEncoding, decompressor);
        }

        // read sync bytes
        syncFirst = input.readLong();
        syncSecond = input.readLong();

        // seek to first sync point withing the specified region, unless the region starts at the beginning
        // of the file.  In that case, the reader owns all row groups up to the first sync point.
        if (offset != 0) {
            // if the specified file region does not contain the start of a sync sequence, this call will close the reader
            seekToFirstRowGroupInRange(offset, length);
        }
    }

    public byte getVersion()
    {
        return version;
    }

    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    public int getColumnCount()
    {
        return columnCount;
    }

    public long getLength()
    {
        return length;
    }

    public long getBytesRead()
    {
        return dataSource.getReadBytes();
    }

    public long getRowsRead()
    {
        return rowsRead;
    }

    public long getReadTimeNanos()
    {
        return dataSource.getReadTimeNanos();
    }

    public Slice getSync()
    {
        Slice sync = Slices.allocate(SIZE_OF_LONG + SIZE_OF_LONG);
        sync.setLong(0, syncFirst);
        sync.setLong(SIZE_OF_LONG, syncSecond);
        return sync;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        rowGroupPosition = 0;
        rowGroupRowCount = 0;
        currentChunkRowCount = 0;
        input.close();
        if (decompressor != null) {
            decompressor.destroy();
        }
    }

    public int advance()
            throws IOException
    {
        if (closed) {
            return -1;
        }
        rowGroupPosition += ColumnData.MAX_SIZE;
        currentChunkRowCount = min(ColumnData.MAX_SIZE, rowGroupRowCount - rowGroupPosition);

        // do we still have rows in the current row group
        if (currentChunkRowCount > 0) {
            return currentChunkRowCount;
        }

        // are we at the end?
        if (input.remaining() == 0) {
            close();
            return -1;
        }

        // read uncompressed size of row group (which is useless information)
        if (input.remaining() < SIZE_OF_INT) {
            throw corrupt("RCFile truncated %s", dataSource);
        }
        int uncompressedRowGroupSize = Integer.reverseBytes(input.readInt());

        // read sequence sync if present
        if (uncompressedRowGroupSize == -1) {
            if (input.remaining() < SIZE_OF_LONG + SIZE_OF_LONG + SIZE_OF_INT) {
                throw corrupt("RCFile truncated %s", dataSource);
            }

            // The full sync sequence is "0xFFFFFFFF syncFirst syncSecond".  If
            // this sequence begins in our segment, we must continue process until the
            // next sync sequence.
            // We have already read the 0xFFFFFFFF above, so we must test the
            // end condition back 4 bytes.
            // NOTE: this decision must agree with RcFileDecoderUtils.findFirstSyncPosition
            if (input.position() - SIZE_OF_INT >= end) {
                close();
                return -1;
            }

            if (syncFirst != input.readLong() || syncSecond != input.readLong()) {
                throw corrupt("Invalid sync in RCFile %s", dataSource);
            }

            // read the useless uncompressed length
            uncompressedRowGroupSize = Integer.reverseBytes(input.readInt());
        }
        if (uncompressedRowGroupSize <= 0) {
            throw corrupt("Invalid uncompressed row group length %s", uncompressedRowGroupSize);
        }

        // read row group header
        int uncompressedHeaderSize = Integer.reverseBytes(input.readInt());
        int compressedHeaderSize = Integer.reverseBytes(input.readInt());
        if (compressedHeaderSize > compressedHeaderBuffer.length()) {
            compressedHeaderBuffer = Slices.allocate(compressedHeaderSize);
        }
        input.readBytes(compressedHeaderBuffer, 0, compressedHeaderSize);

        // decompress row group header
        Slice header;
        if (decompressor != null) {
            if (headerBuffer.length() < uncompressedHeaderSize) {
                headerBuffer = Slices.allocate(uncompressedHeaderSize);
            }
            Slice buffer = headerBuffer.slice(0, uncompressedHeaderSize);

            decompressor.decompress(compressedHeaderBuffer, buffer);

            header = buffer;
        }
        else {
            if (compressedHeaderSize != uncompressedHeaderSize) {
                throw corrupt("Invalid RCFile %s", dataSource);
            }
            header = compressedHeaderBuffer;
        }
        BasicSliceInput headerInput = header.getInput();

        // read number of rows in row group
        rowGroupRowCount = Ints.checkedCast(readVInt(headerInput));
        rowsRead += rowGroupRowCount;
        rowGroupPosition = 0;
        currentChunkRowCount = min(ColumnData.MAX_SIZE, rowGroupRowCount);

        // set column buffers
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int compressedDataSize = Ints.checkedCast(readVInt(headerInput));
            int uncompressedDataSize = Ints.checkedCast(readVInt(headerInput));
            if (decompressor == null && compressedDataSize != uncompressedDataSize) {
                throw corrupt("Invalid RCFile %s", dataSource);
            }

            int lengthsSize = Ints.checkedCast(readVInt(headerInput));

            Slice lengthsBuffer = headerInput.readSlice(lengthsSize);

            Slice dataBuffer;
            if (readColumns.contains(columnIndex)) {
                dataBuffer = input.readSlice(compressedDataSize);
            }
            else {
                skipFully(input, compressedDataSize);
                dataBuffer = Slices.EMPTY_SLICE;
            }

            columns[columnIndex].setBuffers(lengthsBuffer, dataBuffer, uncompressedDataSize);
        }

        return currentChunkRowCount;
    }

    public Block readBlock(int columnIndex)
            throws IOException
    {
        if (currentChunkRowCount <= 0) {
            throw new IllegalStateException("No more data");
        }
        return columns[columnIndex].readBlock(rowGroupPosition, currentChunkRowCount);
    }

    private void seekToFirstRowGroupInRange(long offset, long length)
            throws IOException
    {
        long startOfSyncSequence = findFirstSyncPosition(dataSource, offset, length, syncFirst, syncSecond);
        if (startOfSyncSequence < 0) {
            closeQuietly();
            return;
        }
        input.setPosition(startOfSyncSequence);
    }

    private void closeQuietly()
    {
        try {
            close();
        }
        catch (IOException ignored) {
        }
    }

    private Slice readLengthPrefixedString(SliceInput in)
            throws RcFileCorruptionException
    {
        int length = Ints.checkedCast(readVInt(in));
        if (length > MAX_METADATA_STRING_LENGTH) {
            throw corrupt("Metadata string value is too long (%s) in RCFile %s", length, in);
        }

        return in.readSlice(length);
    }

    private RcFileCorruptionException corrupt(String messageFormat, Object... args)
    {
        closeQuietly();
        return new RcFileCorruptionException(messageFormat, args);
    }

    private static class Column
    {
        private final ColumnEncoding encoding;
        private final RcFileDecompressor decompressor;

        private BasicSliceInput lengthsInput;
        private Slice dataBuffer;
        private int uncompressedDataSize;

        private byte[] decompressedBuffer = new byte[0];

        private boolean compressed;

        private int currentPosition;

        private int currentOffset;
        private int runLength;
        private int lastValueLength = -1;

        public Column(ColumnEncoding encoding, RcFileDecompressor decompressor)
        {
            this.encoding = encoding;
            this.decompressor = decompressor;
        }

        public void setBuffers(Slice lengthsBuffer, Slice dataBuffer, int uncompressedDataSize)
        {
            this.lengthsInput = lengthsBuffer.getInput();
            this.dataBuffer = dataBuffer;
            this.uncompressedDataSize = uncompressedDataSize;

            compressed = (decompressor != null);

            currentPosition = 0;
            currentOffset = 0;
            runLength = 0;
            lastValueLength = 0;
        }

        public Block readBlock(int position, int size)
                throws IOException
        {
            checkArgument(size > 0 && size <= ColumnData.MAX_SIZE, "Invalid size");
            checkArgument(currentPosition <= position, "Invalid position");

            if (currentPosition < position) {
                skipTo(position);
            }

            // read offsets
            int[] offsets = readOffsets(size);

            ColumnData columnData = new ColumnData(offsets, getDataBuffer());
            Block block = encoding.decodeColumn(columnData);
            return block;
        }

        private int[] readOffsets(int batchSize)
                throws IOException
        {
            int[] offsets = new int[batchSize + 1];
            offsets[0] = currentOffset;
            for (int i = 0; i < batchSize; i++) {
                int valueLength = readNextValueLength();
                offsets[i + 1] = offsets[i] + valueLength;
            }
            currentOffset = offsets[batchSize];
            currentPosition += batchSize;
            return offsets;
        }

        private void skipTo(int position)
                throws IOException
        {
            checkArgument(currentPosition <= position, "Invalid position");
            for (; currentPosition < position; currentPosition++) {
                int valueLength = readNextValueLength();
                currentOffset += valueLength;
            }
        }

        private int readNextValueLength()
                throws IOException
        {
            if (runLength > 0) {
                runLength--;
                return lastValueLength;
            }

            int valueLength = Ints.checkedCast(readVInt(lengthsInput));

            // negative length is used to encode a run or the last value
            if (valueLength < 0) {
                if (lastValueLength == -1) {
                    throw new RcFileCorruptionException("First column value length is negative");
                }
                runLength = (~valueLength) - 1;
                return lastValueLength;
            }

            runLength = 0;
            lastValueLength = valueLength;
            return valueLength;
        }

        private Slice getDataBuffer()
                throws IOException
        {
            if (compressed) {
                if (decompressedBuffer.length < uncompressedDataSize) {
                    decompressedBuffer = new byte[uncompressedDataSize];
                }
                Slice buffer = Slices.wrappedBuffer(decompressedBuffer, 0, uncompressedDataSize);

                decompressor.decompress(dataBuffer, buffer);

                dataBuffer = buffer;
                compressed = false;
            }
            return dataBuffer;
        }
    }

    private static class DataSourceSliceLoader
            implements SliceLoader<ByteArrayBufferReference>
    {
        private final RcFileDataSource dataSource;

        public DataSourceSliceLoader(RcFileDataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        @Override
        public ByteArrayBufferReference createBuffer(int bufferSize)
        {
            return new ByteArrayBufferReference(bufferSize);
        }

        @Override
        public long getSize()
        {
            return dataSource.getSize();
        }

        @Override
        public void load(long position, ByteArrayBufferReference bufferReference, int length)
        {
            try {
                dataSource.readFully(position, bufferReference.getByteBuffer(), 0, length);
            }
            catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }

        @Override
        public void close()
        {
            try {
                dataSource.close();
            }
            catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    private static class ByteArrayBufferReference
            implements BufferReference
    {
        private final byte[] byteBuffer;
        private final Slice sliceBuffer;

        public ByteArrayBufferReference(int size)
        {
            byteBuffer = new byte[size];
            sliceBuffer = Slices.wrappedBuffer(byteBuffer);
        }

        public byte[] getByteBuffer()
        {
            return byteBuffer;
        }

        @Override
        public Slice getSlice()
        {
            return sliceBuffer;
        }
    }
}
