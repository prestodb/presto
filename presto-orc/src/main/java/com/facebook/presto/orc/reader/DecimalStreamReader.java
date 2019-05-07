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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.DecimalInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.Int128ArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.ReaderUtils.minNonNullValueSize;
import static com.facebook.presto.orc.reader.ReaderUtils.unpackInt128Nulls;
import static com.facebook.presto.orc.reader.ReaderUtils.unpackLongNulls;
import static com.facebook.presto.orc.reader.ReaderUtils.verifyStreamType;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class DecimalStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalStreamReader.class).instanceSize();

    private final DecimalType type;
    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private boolean[] nullVector = new boolean[0];
    private long[] scaleVector = new long[0];

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<DecimalInputStream> decimalStreamSource = missingStreamSource(DecimalInputStream.class);
    @Nullable
    private DecimalInputStream decimalStream;

    private InputStreamSource<LongInputStream> scaleStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream scaleStream;

    private boolean rowGroupOpen;

    private long[] nonNullValueTemp = new long[0];

    private LocalMemoryContext systemMemoryContext;

    public DecimalStreamReader(Type type, StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(streamDescriptor, type, DecimalType.class::isInstance);
        this.type = (DecimalType) type;
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        seekToOffset();

        Block block;
        if (decimalStream == null && scaleStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            block = RunLengthEncodedBlock.create(type, null, nextBatchSize);
        }
        else if (presentStream == null) {
            checkDataStreamsArePresent();
            block = readNonNullBlock();
        }
        else {
            checkDataStreamsArePresent();
            boolean[] isNull = new boolean[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNull);
            if (nullCount == 0) {
                block = readNonNullBlock();
            }
            else if (nullCount != nextBatchSize) {
                block = readNullBlock(isNull, nextBatchSize - nullCount);
            }
            else {
                block = RunLengthEncodedBlock.create(DOUBLE, null, nextBatchSize);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return block;
    }

    private void checkDataStreamsArePresent()
            throws OrcCorruptionException
    {
        if (decimalStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but decimal stream is missing");
        }
        if (scaleStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but scale stream is missing");
        }
    }

    private Block readNonNullBlock()
            throws IOException
    {
        Block block;
        if (type.isShort()) {
            block = readShortNotNullBlock();
        }
        else {
            block = readLongNotNullBlock();
        }
        return block;
    }

    private Block readShortNotNullBlock()
            throws IOException
    {
        verify(scaleStream != null);
        verify(decimalStream != null);

        long[] data = new long[nextBatchSize];
        decimalStream.nextShortDecimal(data, nextBatchSize);

        for (int i = 0; i < nextBatchSize; i++) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                data[i] = Decimals.rescale(data[i], (int) sourceScale, type.getScale());
            }
        }
        return new LongArrayBlock(nextBatchSize, Optional.empty(), data);
    }

    private Block readLongNotNullBlock()
            throws IOException
    {
        verify(decimalStream != null);
        verify(scaleStream != null);

        long[] data = new long[nextBatchSize * 2];
        decimalStream.nextLongDecimal(data, nextBatchSize);

        for (int offset = 0; offset < data.length; offset += 2) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                Slice decimal = Slices.wrappedLongArray(data[offset], data[offset + 1]);
                UnscaledDecimal128Arithmetic.rescale(decimal, (int) (type.getScale() - sourceScale), Slices.wrappedLongArray(data, offset, 2));
            }
        }
        return new Int128ArrayBlock(nextBatchSize, Optional.empty(), data);
    }

    private Block readNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        Block block;
        if (type.isShort()) {
            block = readShortNullBlock(isNull, nonNullCount);
        }
        else {
            block = readLongNullBlock(isNull, nonNullCount);
        }
        return block;
    }

    private Block readShortNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verify(decimalStream != null);
        verify(scaleStream != null);

        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (nonNullValueTemp.length < minNonNullValueSize) {
            nonNullValueTemp = new long[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp));
        }

        decimalStream.nextShortDecimal(nonNullValueTemp, nonNullCount);

        for (int i = 0; i < nonNullCount; i++) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                nonNullValueTemp[i] = Decimals.rescale(nonNullValueTemp[i], (int) sourceScale, type.getScale());
            }
        }

        long[] result = unpackLongNulls(nonNullValueTemp, isNull);

        return new LongArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private Block readLongNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verify(decimalStream != null);
        verify(scaleStream != null);

        int minTempSize = minNonNullValueSize(nonNullCount) * 2;
        if (nonNullValueTemp.length < minTempSize) {
            nonNullValueTemp = new long[minTempSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp));
        }

        decimalStream.nextLongDecimal(nonNullValueTemp, nonNullCount);

        // rescale if necessary
        for (int offset = 0; offset < nonNullCount * 2; offset += 2) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                Slice decimal = Slices.wrappedLongArray(nonNullValueTemp[offset], nonNullValueTemp[offset + 1]);
                UnscaledDecimal128Arithmetic.rescale(decimal, (int) (type.getScale() - sourceScale), Slices.wrappedLongArray(nonNullValueTemp, offset, 2));
            }
        }

        long[] result = unpackInt128Nulls(nonNullValueTemp, isNull);

        return new Int128ArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        decimalStream = decimalStreamSource.openStream();
        scaleStream = scaleStreamSource.openStream();
        rowGroupOpen = true;
    }

    private void seekToOffset()
            throws IOException
    {
        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                checkDataStreamsArePresent();
                verify(decimalStream != null);
                verify(scaleStream != null);

                decimalStream.skip(readOffset);
                scaleStream.skip(readOffset);
            }
        }
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        decimalStreamSource = missingStreamSource(DecimalInputStream.class);
        scaleStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        decimalStream = null;
        scaleStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        decimalStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, DecimalInputStream.class);
        scaleStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, SECONDARY, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        decimalStream = null;
        scaleStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
        nullVector = null;
        scaleVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector) + sizeOf(scaleVector);
    }
}
