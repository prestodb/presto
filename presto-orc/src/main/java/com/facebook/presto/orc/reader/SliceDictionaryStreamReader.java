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
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.ByteArrayInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.orc.reader.SliceStreamReader.computeTruncatedLength;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SliceDictionaryStreamReader
        implements StreamReader
{
    private static final byte[] EMPTY_DICTIONARY_DATA = new byte[0];
    // add one extra entry for null after strip/rowGroup dictionary
    private static final int[] EMPTY_DICTIONARY_OFFSETS = new int[2];

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;
    private boolean[] isNullVector = new boolean[0];

    @Nonnull
    private InputStreamSource<ByteArrayInputStream> stripeDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private boolean stripeDictionaryOpen;
    private int stripeDictionarySize;
    @Nonnull
    private int[] stripeDictionaryLength = new int[0];
    @Nonnull
    private byte[] stripeDictionaryData = EMPTY_DICTIONARY_DATA;
    @Nonnull
    private int[] stripeDictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;

    private VariableWidthBlock dictionaryBlock = new VariableWidthBlock(1, Slices.wrappedBuffer(EMPTY_DICTIONARY_DATA), EMPTY_DICTIONARY_OFFSETS, Optional.of(new boolean[]{true}));
    private byte[] currentDictionaryData = EMPTY_DICTIONARY_DATA;

    @Nonnull
    private InputStreamSource<LongInputStream> stripeDictionaryLengthStreamSource = missingStreamSource(LongInputStream.class);

    @Nonnull
    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream inDictionaryStream;
    private boolean[] inDictionaryVector = new boolean[0];

    @Nonnull
    private InputStreamSource<ByteArrayInputStream> rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);

    @Nonnull
    private InputStreamSource<RowGroupDictionaryLengthInputStream> rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthInputStream.class);
    @Nonnull
    private int[] rowGroupDictionaryLength = new int[0];

    @Nonnull
    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;

    public SliceDictionaryStreamReader(StreamDescriptor streamDescriptor)
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
            openRowGroup(type);
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                if (inDictionaryStream != null) {
                    inDictionaryStream.skip(readOffset);
                }
                dataStream.skip(readOffset);
            }
        }

        assureVectorSize();

        // idsVector will be filled sub-batch by sub-batch.
        int[] idsVector = new int[nextBatchSize];
        int idsVectorOffset = 0;
        while (nextBatchSize > idsVectorOffset) {
            int subBatchSize = min(nextBatchSize - idsVectorOffset, MAX_BATCH_SIZE);
            if (presentStream == null) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                Arrays.fill(isNullVector, false);
                dataStream.nextIntVector(subBatchSize, idsVector, idsVectorOffset);
            }
            else {
                int nullValues = presentStream.getUnsetBits(subBatchSize, isNullVector);
                if (nullValues != subBatchSize) {
                    if (dataStream == null) {
                        throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                    }
                    dataStream.nextIntVector(subBatchSize, idsVector, idsVectorOffset, isNullVector);
                }
            }

            if (inDictionaryStream == null) {
                Arrays.fill(inDictionaryVector, true);
            }
            else {
                inDictionaryStream.getSetBits(subBatchSize, inDictionaryVector, isNullVector);
            }

            // create the dictionary ids
            for (int i = 0; i < subBatchSize; i++) {
                int dataVectorIndex = i + idsVectorOffset;
                if (isNullVector[i]) {
                    // null is the last entry in the slice dictionary
                    idsVector[dataVectorIndex] = dictionaryBlock.getPositionCount() - 1;
                }
                else if (inDictionaryVector[i]) {
                    // stripe dictionary elements have the same dictionary id
                }
                else {
                    // row group dictionary elements are after the main dictionary
                    idsVector[dataVectorIndex] += stripeDictionarySize;
                }
            }
            idsVectorOffset += subBatchSize;
        }

        Block block = new DictionaryBlock(nextBatchSize, dictionaryBlock, idsVector);

        readOffset = 0;
        nextBatchSize = 0;
        return block;
    }

    private void assureVectorSize()
    {
        int requiredVectorLength = min(nextBatchSize, MAX_BATCH_SIZE);
        if (isNullVector.length < requiredVectorLength) {
            isNullVector = new boolean[requiredVectorLength];
            inDictionaryVector = new boolean[requiredVectorLength];
        }
    }

    private void setDictionaryBlockData(byte[] dictionaryData, int[] dictionaryOffsets, int positionCount)
    {
        verify(positionCount > 0);
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (currentDictionaryData != dictionaryData) {
            boolean[] isNullVector = new boolean[positionCount];
            isNullVector[positionCount - 1] = true;
            dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount - 1];
            dictionaryBlock = new VariableWidthBlock(positionCount, Slices.wrappedBuffer(dictionaryData), dictionaryOffsets, Optional.of(isNullVector));
            currentDictionaryData = dictionaryData;
        }
    }

    private void openRowGroup(Type type)
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
                lengthStream.nextIntVector(stripeDictionarySize, stripeDictionaryLength);

                long dataLength = 0;
                for (int i = 0; i < stripeDictionarySize; i++) {
                    dataLength += stripeDictionaryLength[i];
                }

                // we must always create a new dictionary array because the previous dictionary may still be referenced
                stripeDictionaryData = new byte[toIntExact(dataLength)];
                // add one extra entry for null
                stripeDictionaryOffsetVector = new int[stripeDictionarySize + 2];

                // read dictionary values
                ByteArrayInputStream dictionaryDataStream = stripeDictionaryDataStreamSource.openStream();
                readDictionary(dictionaryDataStream, stripeDictionarySize, stripeDictionaryLength, 0, stripeDictionaryData, stripeDictionaryOffsetVector, type);
            }
            else {
                stripeDictionaryData = EMPTY_DICTIONARY_DATA;
                stripeDictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
            }
        }
        stripeDictionaryOpen = true;

        // read row group dictionary
        RowGroupDictionaryLengthInputStream dictionaryLengthStream = rowGroupDictionaryLengthStreamSource.openStream();
        if (dictionaryLengthStream != null) {
            int rowGroupDictionarySize = dictionaryLengthStream.getEntryCount();

            // resize the dictionary lengths array if necessary
            if (rowGroupDictionaryLength.length < rowGroupDictionarySize) {
                rowGroupDictionaryLength = new int[rowGroupDictionarySize];
            }

            // read the lengths
            dictionaryLengthStream.nextIntVector(rowGroupDictionarySize, rowGroupDictionaryLength);
            long dataLength = 0;
            for (int i = 0; i < rowGroupDictionarySize; i++) {
                dataLength += rowGroupDictionaryLength[i];
            }

            // We must always create a new dictionary array because the previous dictionary may still be referenced
            // The first elements of the dictionary are from the stripe dictionary, then the row group dictionary elements, and then a null
            byte[] rowGroupDictionaryData = Arrays.copyOf(stripeDictionaryData, stripeDictionaryOffsetVector[stripeDictionarySize] + toIntExact(dataLength));
            int[] rowGroupDictionaryOffsetVector = Arrays.copyOf(stripeDictionaryOffsetVector, stripeDictionarySize + rowGroupDictionarySize + 2);

            // read dictionary values
            ByteArrayInputStream dictionaryDataStream = rowGroupDictionaryDataStreamSource.openStream();
            readDictionary(dictionaryDataStream, rowGroupDictionarySize, rowGroupDictionaryLength, stripeDictionarySize, rowGroupDictionaryData, rowGroupDictionaryOffsetVector, type);
            setDictionaryBlockData(rowGroupDictionaryData, rowGroupDictionaryOffsetVector, stripeDictionarySize + rowGroupDictionarySize + 1);
        }
        else {
            // there is no row group dictionary so use the stripe dictionary
            setDictionaryBlockData(stripeDictionaryData, stripeDictionaryOffsetVector, stripeDictionarySize + 1);
        }

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
            Type type)
            throws IOException
    {
        Slice slice = Slices.wrappedBuffer(data);

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
                truncatedLength = computeTruncatedLength(slice, offset, length, type);
                verify(truncatedLength >= 0);
            }
            else {
                truncatedLength = 0;
            }
            offsetVector[offsetIndex + 1] = offsetVector[offsetIndex] + truncatedLength;
        }
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        stripeDictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayInputStream.class);
        stripeDictionaryLengthStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);
        stripeDictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        stripeDictionaryOpen = false;

        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
        rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthInputStream.class);
        rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

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
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
