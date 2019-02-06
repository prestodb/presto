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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.ByteArrayInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SliceStreamReader.computeTruncatedLength;
import static com.facebook.presto.orc.reader.SliceStreamReader.getMaxCodePointCount;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SliceDirectStreamReader
        extends ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDirectStreamReader.class).instanceSize();
    private static final int ONE_GIGABYTE = toIntExact(new DataSize(1, GIGABYTE).toBytes());

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private InputStreamSource<ByteArrayInputStream> dataByteSource = missingStreamSource(ByteArrayInputStream.class);
    @Nullable
    private ByteArrayInputStream dataStream;

    // Content bytes to be returned in Block.
    private byte[] bytes;
    // Start offsets for use in returned Block.
    private int[] resultOffsets;
    // Null flags for use in result Block.
    private boolean[] valueIsNull;
    // Temp space for extracting values to filter when a value straddles buffers.
    private byte[] tempBytes;
    // Result arrays from outputQualifyingSet.
    private int[] outputRows;
    private int[] resultInputNumbers;
    private long totalBytes;
    private long totalRows;

    public SliceDirectStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
    }

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
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but length stream is not present");
                }
                long dataSkipSize = lengthStream.sum(readOffset);
                if (dataSkipSize > 0) {
                    if (dataStream == null) {
                        throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                    }
                    dataStream.skip(dataSkipSize);
                }
            }
        }

        // create new isNullVector and offsetVector for VariableWidthBlock
        boolean[] isNullVector = null;

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];

        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but length stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, offsetVector, 0);
        }
        else {
            isNullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but length stream is not present");
                }

                if (nullValues == 0) {
                    isNullVector = null;
                    lengthStream.nextIntVector(nextBatchSize, offsetVector, 0);
                }
                else {
                    lengthStream.nextIntVector(nextBatchSize, offsetVector, 0, isNullVector);
                }
            }
        }

        // Calculate the total length for all entries. Note that the values in the offsetVector are still length values now.
        long totalLength = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            if (isNullVector == null || !isNullVector[i]) {
                totalLength += offsetVector[i];
            }
        }

        int currentBatchSize = nextBatchSize;
        readOffset = 0;
        nextBatchSize = 0;
        if (totalLength == 0) {
            return new VariableWidthBlock(currentBatchSize, EMPTY_SLICE, offsetVector, Optional.ofNullable(isNullVector));
        }
        if (totalLength > ONE_GIGABYTE) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    format("Values in column \"%s\" are too large to process for Presto. %s column values are larger than 1GB [%s]", streamDescriptor.getFieldName(), nextBatchSize, streamDescriptor.getOrcDataSourceId()));
        }
        if (dataStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
        }

        // allocate enough space to read
        byte[] data = new byte[toIntExact(totalLength)];
        Slice slice = Slices.wrappedBuffer(data);

        // We do the following operations together in the for loop:
        // * truncate strings
        // * convert original length values in offsetVector into truncated offset values
        int currentLength = offsetVector[0];
        offsetVector[0] = 0;
        int maxCodePointCount = getMaxCodePointCount(type);
        boolean isCharType = isCharType(type);
        for (int i = 1; i <= currentBatchSize; i++) {
            int nextLength = offsetVector[i];
            if (isNullVector != null && isNullVector[i - 1]) {
                checkState(currentLength == 0, "Corruption in slice direct stream: length is non-zero for null entry");
                offsetVector[i] = offsetVector[i - 1];
                currentLength = nextLength;
                continue;
            }
            int offset = offsetVector[i - 1];

            // read data without truncation
            dataStream.next(data, offset, offset + currentLength);

            // adjust offsetVector with truncated length
            int truncatedLength = computeTruncatedLength(slice, offset, currentLength, maxCodePointCount, isCharType);
            verify(truncatedLength >= 0);
            offsetVector[i] = offset + truncatedLength;

            currentLength = nextLength;
        }

        // this can lead to over-retention but unlikely to happen given truncation rarely happens
        return new VariableWidthBlock(currentBatchSize, slice, offsetVector, Optional.ofNullable(isNullVector));
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();
        dataStream = dataByteSource.openStream();
        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);
        dataByteSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);
        dataByteSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void erase(int end)
    {
        if (end == 0 || bytes == null) {
            return;
        }
        if (end == numValues) {
            numValues = 0;
            resultOffsets[0] = 0;
            return;
        }
        int firstOffset = resultOffsets[end];
        numValues -= end;
        for (int i = 0; i <= numValues; i++) {
            resultOffsets[i] = resultOffsets[end + i] - firstOffset;
        }
        System.arraycopy(bytes, firstOffset, bytes, 0, resultOffsets[numValues + end] - firstOffset);
        if (valueIsNull != null) {
            System.arraycopy(valueIsNull, end, valueIsNull, 0, numValues);
        }
    }

    @Override
    public void compactValues(int[] positions, int base, int numPositions)
    {
        if (channel != -1) {
            int toOffset = resultOffsets[base];
            for (int i = 0; i < numPositions; i++) {
                int fromPosition = base + positions[i];
                int len = resultOffsets[fromPosition + 1] - resultOffsets[fromPosition];
                System.arraycopy(bytes, resultOffsets[fromPosition], bytes, toOffset, len);
                toOffset += len;
                resultOffsets[base + i + 1] = toOffset;
            }
            if (valueIsNull != null) {
                for (int i = 0; i < numPositions; i++) {
                    valueIsNull[base + i] = valueIsNull[base + positions[i]];
                }
            }
            numValues = base + numPositions;
        }
        compactQualifyingSet(positions, numPositions);
    }

    @Override
    public int getFixedWidth()
    {
        return -1;
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (numValues == 0) {
            return 0;
        }
        return 4 * numValues + resultOffsets[numValues];
    }

    @Override
    public int getAverageResultSize()
    {
        if (totalRows == 0) {
            return 16;
        }
        return (int) ((4 + totalBytes) / totalRows);
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }
        beginScan(presentStream, lengthStream);
        if (resultOffsets == null && channel != -1) {
            resultOffsets = new int[10_001];
            bytes = new byte[100_000];
            numValues = 0;
            resultOffsets[0] = 0;
            resultOffsets[1] = 0;
        }
        long bytesToGo = resultSizeBudget;
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        int end = input.getEnd();
        int rowsInRange = end - positionInRowGroup;

        outputRows = filter != null ? output.getMutablePositions(rowsInRange) : null;
        resultInputNumbers = filter != null ? output.getMutableInputNumbers(rowsInRange) : null;

        int[] inputPositions = input.getPositions();
        int inputPositionCount = input.getPositionCount();
        int inputPositionIndex = 0;

        lengthIndex = 0;
        int toSkip = 0;
        for (int i = 0; i < rowsInRange; i++) {
            int position = i + positionInRowGroup;
            int nextQualifiedPosition = inputPositions[inputPositionIndex];
            verify(position <= nextQualifiedPosition);

            boolean isNull = present != null && !present[i];
            if (position < nextQualifiedPosition) {
                if (!isNull) {
                    toSkip += lengths[lengthIndex++];
                }
                continue;
            }

            if (truncationRow == nextQualifiedPosition) {
                break;
            }

            if (isNull) {
                if (filter == null || filter.testNull()) {
                    if (filter != null) {
                        outputRows[numResults] = position;
                        resultInputNumbers[numResults] = inputPositionIndex;
                    }
                    appendNull();
                }
            }
            else {
                // Non-null row in qualifying set.
                if (toSkip > 0) {
                    dataStream.skip(toSkip);
                    toSkip = 0;
                }
                int length = lengths[lengthIndex];
                if (filter != null) {
                    byte[] buffer = dataStream.getBuffer(length);
                    int pos = 0;
                    if (buffer == null) {
                        // The value to test is not fully contained in the buffer. Read it to tempBytes.
                        if (tempBytes == null || tempBytes.length < length) {
                            tempBytes = new byte[(int) (length * 1.2)];
                        }
                        buffer = tempBytes;
                        dataStream.next(tempBytes, 0, length);
                    }
                    else {
                        pos = dataStream.getOffsetInBuffer();
                        toSkip += length;
                    }
                    if (filter.testBytes(buffer, pos, length)) {
                        outputRows[numResults] = position;
                        resultInputNumbers[numResults] = inputPositionIndex;
                        appendValue(buffer, pos, length);
                        bytesToGo -= length + 4;
                    }
                }
                else {
                    // No filter.
                    appendValueFromStream(length);
                    bytesToGo -= length + 4;
                }
                lengthIndex++;
            }

            inputPositionIndex++;
            if (inputPositionIndex == inputPositionCount) {
                for (int j = i + 1; j < rowsInRange; j++) {
                    if (present == null || present[j]) {
                        toSkip += lengths[lengthIndex++];
                    }
                }
                break;
            }

            if (bytesToGo <= 0) {
                int truncationPosition = input.getNextTruncationPosition(inputPositionIndex);
                if (truncationPosition < inputPositionCount) {
                    truncationRow = inputPositions[truncationPosition];
                }
                input.setTruncationPosition(truncationPosition);
            }
        }

        if (toSkip > 0) {
            dataStream.skip(toSkip);
        }

        if (channel != -1) {
            totalRows += numResults;
            totalBytes += resultOffsets[numValues + numResults] - resultOffsets[numValues];
        }
        endScan(presentStream);
    }

    private void appendNull()
    {
        if (channel == -1) {
            numResults++;
            return;
        }

        if (valueIsNull == null) {
            valueIsNull = new boolean[resultOffsets.length];
        }
        ensureResultRows();
        int position = numResults + numValues;
        valueIsNull[position] = true;
        resultOffsets[position + 1] = resultOffsets[position];

        numResults++;
    }

    private void appendValue(byte[] buffer, int pos, int length)
    {
        if (channel == -1) {
            numResults++;
            return;
        }

        ensureResultBytes(length);
        ensureResultRows();
        int position = numValues + numResults;
        int endOffset = resultOffsets[position];
        System.arraycopy(buffer, pos, bytes, endOffset, length);
        resultOffsets[position + 1] = endOffset + length;
        if (valueIsNull != null) {
            valueIsNull[position] = false;
        }
        numResults++;
    }

    private void appendValueFromStream(int length)
            throws IOException
    {
        if (channel == -1) {
            numResults++;
            return;
        }

        ensureResultBytes(length);
        ensureResultRows();
        int position = numValues + numResults;
        int endOffset = resultOffsets[position];
        // This is an unaccountable perversion and a violation of
        // every principle of consistent design: The argument of next() called
        // length is in fact an end offset into the buffer.
        dataStream.next(bytes, endOffset, endOffset + length);
        resultOffsets[numValues + numResults + 1] = endOffset + length;
        if (valueIsNull != null) {
            valueIsNull[position] = false;
        }
        numResults++;
    }

    private void ensureResultBytes(int length)
    {
        int offset = resultOffsets[numResults + numValues];
        if (bytes.length < length + offset) {
            bytes = Arrays.copyOf(bytes, Math.max(bytes.length * 2, bytes.length + length));
        }
    }

    private void ensureResultRows()
    {
        if (resultOffsets.length <= numValues + numResults + 2) {
            resultOffsets = Arrays.copyOf(resultOffsets, resultOffsets.length * 2);
            if (valueIsNull != null) {
                valueIsNull = Arrays.copyOf(valueIsNull, resultOffsets.length);
            }
        }
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        if (mayReuse) {
            return new VariableWidthBlock(numFirstRows, Slices.wrappedBuffer(bytes), resultOffsets, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull));
        }
        if (numFirstRows < numValues || bytes.length > (int) (resultOffsets[numFirstRows] * 1.2)) {
            int numBytes = resultOffsets[numFirstRows];
            return new VariableWidthBlock(
                    numFirstRows,
                    Slices.wrappedBuffer(Arrays.copyOf(bytes, numBytes)),
                    Arrays.copyOf(resultOffsets, numFirstRows + 1),
                    valueIsNull == null ? Optional.empty() : Optional.of(Arrays.copyOf(valueIsNull, numFirstRows)));
        }
        Block block = new VariableWidthBlock(numFirstRows, Slices.wrappedBuffer(bytes), resultOffsets, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull));
        numValues = 0;
        resultOffsets = null;
        valueIsNull = null;
        bytes = null;
        return block;
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
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
