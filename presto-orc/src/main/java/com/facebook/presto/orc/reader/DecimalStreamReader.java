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
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.DecimalInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.Int128ArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.ResizedArrays.resize;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.StreamReaders.compactArray;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class DecimalStreamReader
        extends ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<DecimalInputStream> decimalStreamSource = missingStreamSource(DecimalInputStream.class);
    @Nullable
    private DecimalInputStream decimalStream;

    private InputStreamSource<LongInputStream> scaleStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream scaleStream;

    private LocalMemoryContext systemMemoryContext;

    public DecimalStreamReader(StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
    {
        super(OptionalInt.empty());
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    private boolean shortDecimal;
    private int numLongsPerValue;
    private int fixedValueSize;
    // One entry per value if decimal is short; two values if decimal is long
    private long[] values;

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        DecimalType decimalType = (DecimalType) type;

        if (!rowGroupOpen) {
            openRowGroup();
        }

        seekToOffset();

        BlockBuilder builder = decimalType.createBlockBuilder(null, nextBatchSize);

        if (presentStream == null) {
            if (decimalStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but decimal stream is not present");
            }
            if (scaleStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but scale stream is not present");
            }

            for (int i = 0; i < nextBatchSize; i++) {
                long sourceScale = scaleStream.next();
                if (decimalType.isShort()) {
                    long rescaledDecimal = Decimals.rescale(decimalStream.nextLong(), (int) sourceScale, decimalType.getScale());
                    decimalType.writeLong(builder, rescaledDecimal);
                }
                else {
                    Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
                    Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();

                    decimalStream.nextLongDecimal(decimal);
                    rescale(decimal, (int) (decimalType.getScale() - sourceScale), rescaledDecimal);
                    decimalType.writeSlice(builder, rescaledDecimal);
                }
            }
        }
        else {
            for (int i = 0; i < nextBatchSize; i++) {
                if (presentStream.nextBit()) {
                    // The current row is not null
                    verify(decimalStream != null);
                    verify(scaleStream != null);
                    long sourceScale = scaleStream.next();
                    if (decimalType.isShort()) {
                        long rescaledDecimal = Decimals.rescale(decimalStream.nextLong(), (int) sourceScale, decimalType.getScale());
                        decimalType.writeLong(builder, rescaledDecimal);
                    }
                    else {
                        Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
                        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();

                        decimalStream.nextLongDecimal(decimal);
                        rescale(decimal, (int) (decimalType.getScale() - sourceScale), rescaledDecimal);
                        decimalType.writeSlice(builder, rescaledDecimal);
                    }
                }
                else {
                    builder.appendNull();
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return builder.build();
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        decimalStream = decimalStreamSource.openStream();
        scaleStream = scaleStreamSource.openStream();
        super.openRowGroup();
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
                if (decimalStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but decimal stream is not present");
                }
                if (scaleStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but scale stream is not present");
                }

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
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void setFilterAndChannel(Filter filter, int channel, int columnIndex, Type type)
    {
        checkArgument(type instanceof DecimalType, "Unsupported type: " + type.getDisplayName());
        shortDecimal = ((DecimalType) type).isShort();
        numLongsPerValue = shortDecimal ? 1 : 2;
        fixedValueSize = numLongsPerValue * SIZE_OF_LONG;
        super.setFilterAndChannel(filter, channel, columnIndex, type);
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        int decimalScale = ((DecimalType) type).getScale();

        beginScan(presentStream, null);
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        int end = input.getEnd();
        int rowsInRange = end - posInRowGroup;
        int[] inputPositions = input.getPositions();
        if (filter != null) {
            output.reset(rowsInRange);
        }
        ensureValuesCapacity(end * numLongsPerValue, false);

        if (decimalStream == null) {
            processAllNulls();
            endScan(presentStream);
            return;
        }

        int nextActive = inputPositions[0];
        int activeIdx = 0;
        int toSkip = 0;
        for (int i = 0; i < rowsInRange; i++) {
            if (i + posInRowGroup == nextActive) {
                if (presentStream != null && !present[i]) {
                    if (filter == null || filter.testNull()) {
                        if (filter != null) {
                            output.append(i + posInRowGroup, activeIdx);
                        }
                        addNullResult();
                    }
                }
                else {
                    // Non-null row in qualifying set.
                    if (toSkip > 0) {
                        decimalStream.skip(toSkip);
                        scaleStream.skip(toSkip);
                    }

                    long sourceScale = scaleStream.next();
                    if (shortDecimal) {
                        long value = Decimals.rescale(decimalStream.nextLong(), (int) sourceScale, decimalScale);
                        if (filter != null) {
                            if (filter.testLong(value)) {
                                output.append(i + posInRowGroup, activeIdx);
                                addResult(value);
                            }
                        }
                        else {
                            // No filter.
                            addResult(value);
                        }
                    }
                    else {
                        Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
                        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();

                        decimalStream.nextLongDecimal(decimal);
                        rescale(decimal, (int) (decimalScale - sourceScale), rescaledDecimal);

                        if (filter != null) {
                            if (filter.testDecimal(rescaledDecimal.getLong(0), rescaledDecimal.getLong(SIZE_OF_LONG))) {
                                output.append(i + posInRowGroup, activeIdx);
                                addResult(rescaledDecimal);
                            }
                        }
                        else {
                            // No filter.
                            addResult(rescaledDecimal);
                        }
                    }
                }
                if (++activeIdx == input.getPositionCount()) {
                    toSkip = countPresent(i + 1, end - posInRowGroup);
                    break;
                }
                nextActive = inputPositions[activeIdx];
                if (outputChannelSet && numResults * numLongsPerValue > resultSizeBudget) {
                    throw batchTooLarge();
                }
                continue;
            }
            else {
                // The row is not in the input qualifying set. Add to skip if non-null.
                if (presentStream == null || present[i]) {
                    toSkip++;
                }
            }
        }
        if (toSkip > 0) {
            decimalStream.skip(toSkip);
            scaleStream.skip(toSkip);
        }
        endScan(presentStream);
    }

    private void addResult(long value)
    {
        if (!outputChannelSet) {
            return;
        }

        int position = numValues + numResults;
        ensureValuesCapacity(position + 1, false);
        values[position] = value;
        if (valueIsNull != null) {
            valueIsNull[position] = false;
        }
        numResults++;
    }

    private void addResult(Slice value)
    {
        if (!outputChannelSet) {
            return;
        }

        int position = numValues + numResults;
        ensureValuesCapacity(position + 1, false);
        values[2 * position] = value.getLong(0);
        values[2 * position + 1] = value.getLong(SIZE_OF_LONG);
        if (valueIsNull != null) {
            valueIsNull[position] = false;
        }

        numResults++;
    }

    @Override
    protected void ensureValuesCapacity(int capacity, boolean includeNulls)
    {
        int scaledCapacity = capacity * numLongsPerValue;
        if (values == null || values.length < scaledCapacity) {
            values = resize(values, scaledCapacity);
            if (valueIsNull != null) {
                valueIsNull = resize(valueIsNull, capacity);
            }
        }
        if (includeNulls && valueIsNull == null) {
            valueIsNull = new boolean[values.length];
        }
    }

    @Override
    public int getResultSizeInBytes()
    {
        return numValues * fixedValueSize;
    }

    @Override
    public int getAverageResultSize()
    {
        return fixedValueSize;
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        checkEnoughValues(numFirstRows);
        if (shortDecimal) {
            if (mayReuse) {
                return new LongArrayBlock(numFirstRows, Optional.ofNullable(valueIsNull), values);
            }
            if (numFirstRows < numValues || values.length > (int) (numFirstRows * 1.2)) {
                return new LongArrayBlock(
                        numFirstRows,
                        Optional.ofNullable(valueIsNull).map(buffer -> Arrays.copyOf(buffer, numFirstRows)),
                        Arrays.copyOf(values, numFirstRows));
            }
            Block block = new LongArrayBlock(numFirstRows, Optional.ofNullable(valueIsNull), values);
            values = null;
            valueIsNull = null;
            numValues = 0;
            return block;
        }

        if (mayReuse) {
            return new Int128ArrayBlock(
                    numFirstRows,
                    Optional.ofNullable(valueIsNull),
                    values);
        }
        if (numFirstRows < numValues || values.length > (int) (numFirstRows * numLongsPerValue * 1.2)) {
            return new Int128ArrayBlock(
                    numFirstRows,
                    Optional.ofNullable(valueIsNull).map(buffer -> Arrays.copyOf(buffer, numFirstRows)),
                    Arrays.copyOf(values, numFirstRows * numLongsPerValue));
        }
        Block block = new Int128ArrayBlock(
                numFirstRows,
                Optional.ofNullable(valueIsNull),
                values);
        values = null;
        valueIsNull = null;
        numValues = 0;
        return block;
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannelSet) {
            if (shortDecimal) {
                StreamReaders.compactArrays(surviving, base, numSurviving, values, valueIsNull);
            }
            else {
                for (int i = 0; i < numSurviving; i++) {
                    values[2 * (base + i)] = values[2 * (base + surviving[i])];
                    values[2 * (base + i) + 1] = values[2 * (base + surviving[i]) + 1];
                }
                compactArray(surviving, base, numSurviving, valueIsNull);
            }
            numValues = base + numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    @Override
    public void erase(int numFirstRows)
    {
        if (values == null) {
            return;
        }
        numValues -= numFirstRows;
        if (numValues > 0) {
            System.arraycopy(values, numFirstRows * numLongsPerValue, values, 0, numValues * numLongsPerValue);
            if (valueIsNull != null) {
                System.arraycopy(valueIsNull, numFirstRows, valueIsNull, 0, numValues);
            }
        }
    }
}
