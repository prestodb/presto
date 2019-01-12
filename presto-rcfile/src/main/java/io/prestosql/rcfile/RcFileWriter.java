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
package io.prestosql.rcfile;

import com.google.common.io.Closer;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.prestosql.rcfile.RcFileCompressor.CompressedSliceOutput;
import io.prestosql.rcfile.RcFileWriteValidation.RcFileWriteValidationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.rcfile.PageSplitterUtil.splitPage;
import static io.prestosql.rcfile.RcFileDecoderUtils.writeLengthPrefixedString;
import static io.prestosql.rcfile.RcFileDecoderUtils.writeVInt;
import static io.prestosql.rcfile.RcFileReader.validateFile;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

public class RcFileWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RcFileWriter.class).instanceSize();
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
    private final List<Type> types;
    private final RcFileEncoding encoding;
    private final RcFileCodecFactory codecFactory;

    private final long syncFirst = ThreadLocalRandom.current().nextLong();
    private final long syncSecond = ThreadLocalRandom.current().nextLong();

    private CompressedSliceOutput keySectionOutput;
    private final ColumnEncoder[] columnEncoders;

    private final int targetMinRowGroupSize;
    private final int targetMaxRowGroupSize;

    private int bufferedSize;
    private int bufferedRows;

    private long totalRowCount;

    @Nullable
    private final RcFileWriteValidationBuilder validationBuilder;

    public RcFileWriter(
            SliceOutput output,
            List<Type> types,
            RcFileEncoding encoding,
            Optional<String> codecName,
            RcFileCodecFactory codecFactory,
            Map<String, String> metadata,
            boolean validate)
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
                DEFAULT_TARGET_MAX_ROW_GROUP_SIZE,
                validate);
    }

    public RcFileWriter(
            SliceOutput output,
            List<Type> types,
            RcFileEncoding encoding,
            Optional<String> codecName,
            RcFileCodecFactory codecFactory,
            Map<String, String> metadata,
            DataSize targetMinRowGroupSize,
            DataSize targetMaxRowGroupSize,
            boolean validate)
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

        this.validationBuilder = validate ? new RcFileWriteValidationBuilder(types) : null;

        this.output = output;
        this.types = types;
        this.encoding = encoding;
        this.codecFactory = codecFactory;

        // write header
        output.writeBytes(RCFILE_MAGIC);
        output.writeByte(CURRENT_VERSION);
        recordValidation(validation -> validation.setVersion((byte) CURRENT_VERSION));

        // write codec information
        output.writeBoolean(codecName.isPresent());
        codecName.ifPresent(name -> writeLengthPrefixedString(output, utf8Slice(name)));
        recordValidation(validation -> validation.setCodecClassName(codecName));

        // write metadata
        output.writeInt(Integer.reverseBytes(metadata.size() + 2));
        writeMetadataProperty(COLUMN_COUNT_METADATA_KEY, Integer.toString(types.size()));
        writeMetadataProperty(PRESTO_RCFILE_WRITER_VERSION_METADATA_KEY, PRESTO_RCFILE_WRITER_VERSION);
        for (Entry<String, String> entry : metadata.entrySet()) {
            writeMetadataProperty(entry.getKey(), entry.getValue());
        }

        // write sync sequence
        output.writeLong(syncFirst);
        recordValidation(validation -> validation.setSyncFirst(syncFirst));
        output.writeLong(syncSecond);
        recordValidation(validation -> validation.setSyncSecond(syncSecond));

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

    private void writeMetadataProperty(String key, String value)
    {
        writeLengthPrefixedString(output, utf8Slice(key));
        writeLengthPrefixedString(output, utf8Slice(value));
        recordValidation(validation -> validation.addMetadataProperty(key, value));
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(output);
            closer.register(keySectionOutput::destroy);
            for (ColumnEncoder columnEncoder : columnEncoders) {
                closer.register(columnEncoder::destroy);
            }
            writeRowGroup();
        }
    }

    private void recordValidation(Consumer<RcFileWriteValidationBuilder> task)
    {
        if (validationBuilder != null) {
            task.accept(validationBuilder);
        }
    }

    public void validate(RcFileDataSource input)
            throws RcFileCorruptionException
    {
        checkState(validationBuilder != null, "validation is not enabled");
        validateFile(
                validationBuilder.build(),
                input,
                encoding,
                types,
                codecFactory);
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSize = INSTANCE_SIZE;
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
        List<Page> pages = splitPage(page, targetMaxRowGroupSize);
        for (Page splitPage : pages) {
            bufferPage(splitPage);
        }
    }

    private void bufferPage(Page page)
            throws IOException
    {
        bufferedRows += page.getPositionCount();

        bufferedSize = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            columnEncoders[i].writeBlock(block);
            bufferedSize += columnEncoders[i].getBufferedSize();
        }
        recordValidation(validation -> validation.addPage(page));

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
        int valueLength = 0;
        keySectionOutput = keySectionOutput.createRecycledCompressedSliceOutput();
        try {
            writeVInt(keySectionOutput, bufferedRows);
            recordValidation(validation -> validation.addRowGroup(bufferedRows));
            for (ColumnEncoder columnEncoder : columnEncoders) {
                valueLength += columnEncoder.getCompressedSize();
                writeVInt(keySectionOutput, columnEncoder.getCompressedSize());
                writeVInt(keySectionOutput, columnEncoder.getUncompressedSize());

                Slice lengthData = columnEncoder.getLengthData();
                writeVInt(keySectionOutput, lengthData.length());
                keySectionOutput.writeBytes(lengthData);
            }
        }
        finally {
            keySectionOutput.close();
        }

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
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ColumnEncoder.class).instanceSize() + ClassLayout.parseClass(ColumnEncodeOutput.class).instanceSize();

        private final ColumnEncoding columnEncoding;

        private ColumnEncodeOutput encodeOutput;

        private final SliceOutput lengthOutput = new DynamicSliceOutput(512);

        private CompressedSliceOutput output;

        private boolean columnClosed;

        public ColumnEncoder(ColumnEncoding columnEncoding, RcFileCompressor compressor)
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
        {
            checkArgument(columnClosed, "Column is open");
            lengthOutput.reset();

            output = output.createRecycledCompressedSliceOutput();
            encodeOutput = new ColumnEncodeOutput(lengthOutput, output);

            columnClosed = false;
        }

        public void destroy()
                throws IOException
        {
            output.destroy();
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + lengthOutput.getRetainedSize() + output.getRetainedSize();
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
