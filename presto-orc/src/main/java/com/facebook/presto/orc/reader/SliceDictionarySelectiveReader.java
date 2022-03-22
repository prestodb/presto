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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.ByteArrayInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthInputStream;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.MEDIUM;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.CHAR;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.reader.SliceSelectiveStreamReader.computeTruncatedLength;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class SliceDictionarySelectiveReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionarySelectiveReader.class).instanceSize();

    // filter evaluation states, using byte constants instead of enum as its memory efficient
    private static final byte FILTER_NOT_EVALUATED = 0;
    private static final byte FILTER_PASSED = 1;
    private static final byte FILTER_FAILED = 2;

    private static final byte[] EMPTY_DICTIONARY_DATA = new byte[0];
    // add one extra entry for null after stripe/rowGroup dictionary
    private static final int[] EMPTY_DICTIONARY_OFFSETS = new int[2];

    // Each rowgroup has roughly 10K rows, and each batch reads 1K rows. So there're about 10 batches in a rowgroup.
    private static final int BATCHES_PER_ROWGROUP = 10;
    // MATERIALIZATION_RATIO should be greater than or equal to 1.0f to compensate the extra CPU to materialize blocks.
    private static final float MATERIALIZATION_RATIO = 2.0f;

    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;
    private final Type outputType;
    private final boolean outputRequired;
    private final StreamDescriptor streamDescriptor;
    private final int maxCodePointCount;
    private final boolean isCharType;

    private byte[] dictionaryData = EMPTY_DICTIONARY_DATA;
    private int[] dictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
    private int[] stripeDictionaryLength = new int[0];
    private int[] rowGroupDictionaryLength = new int[0];
    private byte[] evaluationStatus;
    private byte[] valueWithPadding;

    private int readOffset;

    private VariableWidthBlock dictionary = new VariableWidthBlock(1, wrappedBuffer(EMPTY_DICTIONARY_DATA), EMPTY_DICTIONARY_OFFSETS, Optional.of(new boolean[] {true}));

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private BooleanInputStream presentStream;

    private BooleanInputStream inDictionaryStream;

    private InputStreamSource<ByteArrayInputStream> stripeDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private InputStreamSource<LongInputStream> stripeDictionaryLengthStreamSource = missingStreamSource(LongInputStream.class);
    private boolean stripeDictionaryOpen;
    // The dictionaries will be wrapped in getBlock(). It's set to false when opening a new dictionary (be it stripe dictionary or rowgroup dictionary). When there is only stripe
    // dictionary but no rowgroup dictionaries, we shall set it to false only when opening the stripe dictionary while not for every rowgroup. It is set to true when the dictionary
    // is wrapped up in wrapDictionaryIfNecessary().
    private boolean dictionaryWrapped;

    private int stripeDictionarySize;
    private int currentDictionarySize;

    private InputStreamSource<ByteArrayInputStream> rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<RowGroupDictionaryLengthInputStream> rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthInputStream.class);

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    private LongInputStream dataStream;

    private boolean rowGroupOpen;
    private OrcLocalMemoryContext systemMemoryContext;

    private int[] values;
    private boolean allNulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean valuesInUse;

    public SliceDictionarySelectiveReader(StreamDescriptor streamDescriptor, Optional<TupleDomainFilter> filter, Optional<Type> outputType, OrcLocalMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.systemMemoryContext = systemMemoryContext;
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
        OrcType orcType = streamDescriptor.getOrcType();
        this.maxCodePointCount = orcType == null ? 0 : orcType.getLength().orElse(-1);
        this.valueWithPadding = maxCodePointCount < 0 ? null : new byte[maxCodePointCount];
        this.isCharType = orcType.getOrcTypeKind() == CHAR;
        this.outputRequired = outputType.isPresent();
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if outputRequired is false");
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            values = ensureCapacity(values, positionCount);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        outputPositionCount = 0;
        int streamPosition;

        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (filter == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            streamPosition = readWithFilter(positions, positionCount);
        }

        readOffset = offset + streamPosition;

        return outputPositionCount;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                values[i] = currentDictionarySize - 1;
            }
            else {
                boolean isInRowDictionary = inDictionaryStream != null && !inDictionaryStream.nextBit();
                int index = toIntExact(dataStream.next());
                values[i] = isInRowDictionary ? stripeDictionarySize + index : index;
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                    if (outputRequired) {
                        values[outputPositionCount] = currentDictionarySize - 1;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                boolean inRowDictionary = inDictionaryStream != null && !inDictionaryStream.nextBit();
                int rawIndex = toIntExact(dataStream.next());
                int index = inRowDictionary ? stripeDictionarySize + rawIndex : rawIndex;
                int length;
                if (isCharType) {
                    length = maxCodePointCount;
                }
                else {
                    length = inRowDictionary ? rowGroupDictionaryLength[rawIndex] : stripeDictionaryLength[rawIndex];
                }
                if (nonDeterministicFilter) {
                    evaluateFilter(position, index, length);
                }
                else {
                    switch (evaluationStatus[index]) {
                        case FILTER_FAILED: {
                            break;
                        }
                        case FILTER_PASSED: {
                            if (outputRequired) {
                                values[outputPositionCount] = index;
                            }
                            outputPositions[outputPositionCount] = position;
                            outputPositionCount++;
                            break;
                        }
                        case FILTER_NOT_EVALUATED: {
                            evaluationStatus[index] = evaluateFilter(position, index, length);
                            break;
                        }
                        default: {
                            throw new IllegalStateException("invalid evaluation state");
                        }
                    }
                }
            }
            streamPosition++;

            if (filter != null) {
                outputPositionCount -= filter.getPrecedingPositionsToFail();
                int succeedingPositionsToFail = filter.getSucceedingPositionsToFail();
                if (succeedingPositionsToFail > 0) {
                    int positionsToSkip = 0;
                    for (int j = 0; j < succeedingPositionsToFail; j++) {
                        i++;
                        int nextPosition = positions[i];
                        positionsToSkip += 1 + nextPosition - streamPosition;
                        streamPosition = nextPosition + 1;
                    }
                    skip(positionsToSkip);
                }
            }
        }
        return streamPosition;
    }

    private byte evaluateFilter(int position, int index, int length)
    {
        if (!filter.testLength(length)) {
            return FILTER_FAILED;
        }

        int currentLength = dictionaryOffsetVector[index + 1] - dictionaryOffsetVector[index];
        if (isCharType && length != currentLength) {
            System.arraycopy(dictionaryData, dictionaryOffsetVector[index], valueWithPadding, 0, currentLength);
            Arrays.fill(valueWithPadding, currentLength, length, (byte) ' ');
            if (!filter.testBytes(valueWithPadding, 0, length)) {
                return FILTER_FAILED;
            }
        }
        else if (!filter.testBytes(dictionaryData, dictionaryOffsetVector[index], length)) {
            return FILTER_FAILED;
        }

        if (outputRequired) {
            values[outputPositionCount] = index;
        }
        outputPositions[outputPositionCount] = position;
        outputPositionCount++;
        return FILTER_PASSED;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        if (nonDeterministicFilter) {
            outputPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (filter.testNull()) {
                    outputPositionCount++;
                }
                else {
                    outputPositionCount -= filter.getPrecedingPositionsToFail();
                    i += filter.getSucceedingPositionsToFail();
                }
            }
        }
        else if (nullsAllowed) {
            outputPositionCount = positionCount;
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private void skip(int items)
            throws IOException
    {
        if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(dataToSkip);
            }
            if (dataStream != null) {
                dataStream.skip(dataToSkip);
            }
        }
        else {
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(items);
            }
            dataStream.skip(items);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount);
        }

        // compact values(ids) array, and calculate 1) the slice sizeInBytes if materialized, and 2) number of nulls
        compactValues(positions, positionCount);

        long blockSizeInBytes = 0;
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int id = values[i];
            blockSizeInBytes += dictionaryOffsetVector[id + 1] - dictionaryOffsetVector[id];
            if (id == currentDictionarySize - 1) {
                nullCount++;
            }
        }

        // If all selected positions are null, just return RLE block.
        if (nullCount == positionCount) {
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount);
        }

        // If the expected materialized size of the output block is smaller than a certain ratio of the dictionary size, we will materialize the values
        int dictionarySizeInBytes = dictionaryOffsetVector[currentDictionarySize - 1];
        if (blockSizeInBytes * BATCHES_PER_ROWGROUP < dictionarySizeInBytes / MATERIALIZATION_RATIO) {
            return getMaterializedBlock(positionCount, blockSizeInBytes, nullCount);
        }

        wrapDictionaryIfNecessary();

        int[] valuesCopy = Arrays.copyOf(values, positionCount);
        return new DictionaryBlock(positionCount, dictionary, valuesCopy);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount));
        }
        if (positionCount < outputPositionCount) {
            compactValues(positions, positionCount);
        }
        wrapDictionaryIfNecessary();
        return newLease(new DictionaryBlock(positionCount, dictionary, values));
    }

    private void wrapDictionaryIfNecessary()
    {
        if (dictionaryWrapped) {
            return;
        }

        boolean[] isNullVector = new boolean[currentDictionarySize];
        isNullVector[currentDictionarySize - 1] = true;

        byte[] dictionaryDataCopy = Arrays.copyOf(dictionaryData, dictionaryOffsetVector[currentDictionarySize]);
        int[] dictionaryOffsetVectorCopy = Arrays.copyOf(dictionaryOffsetVector, currentDictionarySize + 1);
        dictionary = new VariableWidthBlock(currentDictionarySize, wrappedBuffer(dictionaryDataCopy), dictionaryOffsetVectorCopy, Optional.of(isNullVector));

        dictionaryWrapped = true;
    }

    private void compactValues(int[] positions, int positionCount)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            values[positionIndex] = values[i];
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    private void openRowGroup()
            throws IOException
    {
        // read the dictionary
        if (!stripeDictionaryOpen) {
            if (stripeDictionarySize > 0) {
                // resize the dictionary lengths array if necessary
                if (stripeDictionaryLength.length < stripeDictionarySize) {
                    stripeDictionaryLength = new int[stripeDictionarySize];
                }

                // read the lengths
                LongInputStream lengthStream = stripeDictionaryLengthStreamSource.openStream();
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Dictionary is not empty but dictionary length stream is not present");
                }
                lengthStream.nextIntVector(stripeDictionarySize, stripeDictionaryLength, 0);

                long dataLength = 0;
                for (int i = 0; i < stripeDictionarySize; i++) {
                    dataLength += stripeDictionaryLength[i];
                }

                dictionaryData = ensureCapacity(dictionaryData, toIntExact(dataLength));
                dictionaryOffsetVector = ensureCapacity(dictionaryOffsetVector, stripeDictionarySize + 2);

                // read dictionary values
                ByteArrayInputStream dictionaryDataStream = stripeDictionaryDataStreamSource.openStream();
                readDictionary(dictionaryDataStream, stripeDictionarySize, stripeDictionaryLength, 0, dictionaryData, dictionaryOffsetVector, maxCodePointCount, isCharType);
            }
            else {
                dictionaryData = EMPTY_DICTIONARY_DATA;
                dictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
            }

            // If there is no rowgroup dictionary, we only need to wrap the stripe dictionary once per stripe because wrapping dictionary is very expensive.
            dictionaryWrapped = false;
        }

        // read row group dictionary
        RowGroupDictionaryLengthInputStream dictionaryLengthStream = rowGroupDictionaryLengthStreamSource.openStream();
        if (dictionaryLengthStream != null) {
            int rowGroupDictionarySize = dictionaryLengthStream.getEntryCount();

            rowGroupDictionaryLength = ensureCapacity(rowGroupDictionaryLength, rowGroupDictionarySize);

            // read the lengths
            dictionaryLengthStream.nextIntVector(rowGroupDictionarySize, rowGroupDictionaryLength, 0);
            long dataLength = 0;
            for (int i = 0; i < rowGroupDictionarySize; i++) {
                dataLength += rowGroupDictionaryLength[i];
            }

            dictionaryData = ensureCapacity(
                    dictionaryData,
                    dictionaryOffsetVector[stripeDictionarySize] + toIntExact(dataLength),
                    MEDIUM,
                    PRESERVE);

            dictionaryOffsetVector = ensureCapacity(dictionaryOffsetVector,
                    stripeDictionarySize + rowGroupDictionarySize + 2,
                    MEDIUM,
                    PRESERVE);

            dictionaryWrapped = false;

            // read dictionary values
            ByteArrayInputStream dictionaryDataStream = rowGroupDictionaryDataStreamSource.openStream();
            readDictionary(dictionaryDataStream, rowGroupDictionarySize, rowGroupDictionaryLength, stripeDictionarySize, dictionaryData, dictionaryOffsetVector, maxCodePointCount, isCharType);
            currentDictionarySize = stripeDictionarySize + rowGroupDictionarySize + 1;

            initiateEvaluationStatus(stripeDictionarySize + rowGroupDictionarySize + 1);
        }
        else {
            // there is no row group dictionary so use the stripe dictionary
            currentDictionarySize = stripeDictionarySize + 1;
            initiateEvaluationStatus(stripeDictionarySize + 1);
        }

        dictionaryOffsetVector[currentDictionarySize] = dictionaryOffsetVector[currentDictionarySize - 1];
        stripeDictionaryOpen = true;
        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    // Reads dictionary into data and offsetVector
    private static void readDictionary(
            @Nullable ByteArrayInputStream dictionaryDataStream,
            int dictionarySize,
            int[] dictionaryLengthVector,
            int offsetVectorOffset,
            byte[] data,
            int[] offsetVector,
            int maxCodePointCount,
            boolean isCharType)
            throws IOException
    {
        Slice slice = wrappedBuffer(data);

        // initialize the offset if necessary;
        // otherwise, use the previous offset
        if (offsetVectorOffset == 0) {
            offsetVector[0] = 0;
        }

        // truncate string and update offsets
        for (int i = 0; i < dictionarySize; i++) {
            int offsetIndex = offsetVectorOffset + i;
            int offset = offsetVector[offsetIndex];
            int length = dictionaryLengthVector[i];

            int truncatedLength;
            if (length > 0) {
                // read data without truncation
                dictionaryDataStream.next(data, offset, offset + length);
                // adjust offsets with truncated length
                truncatedLength = computeTruncatedLength(slice, offset, length, maxCodePointCount, isCharType);
                verify(truncatedLength >= 0);
            }
            else {
                truncatedLength = 0;
            }
            offsetVector[offsetIndex + 1] = offsetVector[offsetIndex] + truncatedLength;
        }
    }

    @Override
    public void startStripe(Stripe stripe)
    {
        InputStreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
        stripeDictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayInputStream.class);
        stripeDictionaryLengthStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);
        stripeDictionarySize = stripe.getColumnEncodings().get(streamDescriptor.getStreamId())
                .getColumnEncoding(streamDescriptor.getSequence())
                .getDictionarySize();
        stripeDictionaryOpen = false;

        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
        rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthInputStream.class);
        rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        // the "in dictionary" stream signals if the value is in the stripe or row group dictionary
        inDictionaryStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, IN_DICTIONARY, BooleanInputStream.class);
        rowGroupDictionaryLengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY_LENGTH, RowGroupDictionaryLengthInputStream.class);
        rowGroupDictionaryDataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY, ByteArrayInputStream.class);

        readOffset = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void close()
    {
        dictionary = null;
        dictionaryData = null;
        dictionaryOffsetVector = null;
        rowGroupDictionaryLength = null;
        stripeDictionaryLength = null;
        values = null;
        outputPositions = null;
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(values) +
                sizeOf(dictionaryData) +
                sizeOf(dictionaryOffsetVector) +
                sizeOf(stripeDictionaryLength) +
                sizeOf(rowGroupDictionaryLength) +
                sizeOf(evaluationStatus) +
                sizeOf(valueWithPadding) +
                dictionary.getRetainedSizeInBytes();
    }

    @VisibleForTesting
    public void resetDataStream()
    {
        dataStream = null;
    }

    private void initiateEvaluationStatus(int positionCount)
    {
        verify(positionCount > 0);

        evaluationStatus = ensureCapacity(evaluationStatus, positionCount - 1);
        fill(evaluationStatus, 0, evaluationStatus.length, FILTER_NOT_EVALUATED);
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    private Block getMaterializedBlock(int positionCount, long blockSizeInBytes, int nullCount)
    {
        byte[] sliceData = new byte[toIntExact(blockSizeInBytes)];
        int[] offsetVector = new int[positionCount + 1];
        int currentOffset = 0;
        for (int i = 0; i < positionCount; i++) {
            int id = values[i];
            int offset = dictionaryOffsetVector[id];
            int length = dictionaryOffsetVector[id + 1] - offset;
            System.arraycopy(dictionaryData, offset, sliceData, currentOffset, length);

            currentOffset += length;
            offsetVector[i + 1] = currentOffset;
        }

        if (nullCount > 0) {
            boolean[] isNullVector = new boolean[positionCount];
            for (int i = 0; i < positionCount; i++) {
                if (values[i] == currentDictionarySize - 1) {
                    isNullVector[i] = true;
                }
            }
            return new VariableWidthBlock(positionCount, wrappedBuffer(sliceData), offsetVector, Optional.of(isNullVector));
        }
        else {
            return new VariableWidthBlock(positionCount, wrappedBuffer(sliceData), offsetVector, Optional.empty());
        }
    }
}
