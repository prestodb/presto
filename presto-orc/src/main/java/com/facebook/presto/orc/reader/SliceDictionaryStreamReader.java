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
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Vector;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.ByteArrayStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.orc.reader.OrcReaderUtils.castOrcVector;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SliceDictionaryStreamReader
        implements StreamReader
{
    private static final int CHUNK_BYTES = 8 * 1024 * 1024; // 8MiB
    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;
    private final boolean[] isNullVector = new boolean[Vector.MAX_VECTOR_LENGTH];

    @Nonnull
    private StreamSource<ByteArrayStream> dictionaryDataStreamSource = missingStreamSource(ByteArrayStream.class);
    private boolean dictionaryOpen;
    private int dictionarySize;
    @Nonnull
    private Slice[] dictionary = new Slice[0];
    private boolean useSharedByteArrayForDictionary;

    @Nonnull
    private StreamSource<LongStream> dictionaryLengthStreamSource = missingStreamSource(LongStream.class);
    @Nonnull
    private int[] dictionaryLength = new int[0];

    @Nonnull
    private StreamSource<BooleanStream> inDictionaryStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream inDictionaryStream;
    private final boolean[] inDictionary = new boolean[Vector.MAX_VECTOR_LENGTH];

    @Nonnull
    private StreamSource<ByteArrayStream> rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayStream.class);
    @Nonnull
    private Slice[] rowGroupDictionary = new Slice[0];

    @Nonnull
    private StreamSource<RowGroupDictionaryLengthStream> rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthStream.class);
    @Nonnull
    private int[] rowGroupDictionaryLength = new int[0];

    @Nonnull
    private StreamSource<LongStream> dataStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream dataStream;
    @Nonnull
    private final int[] dataVector = new int[Vector.MAX_VECTOR_LENGTH];

    private boolean rowGroupOpen;

    public SliceDictionaryStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
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
                if (dataStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                if (inDictionaryStream != null) {
                    inDictionaryStream.skip(readOffset);
                }
                dataStream.skip(readOffset);
            }
        }

        SliceVector sliceVector = castOrcVector(vector, SliceVector.class);

        if (presentStream == null) {
            if (dataStream == null) {
                throw new OrcCorruptionException("Value is not null but data stream is not present");
            }
            Arrays.fill(isNullVector, false);
            dataStream.nextIntVector(nextBatchSize, dataVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullValues != nextBatchSize) {
                if (dataStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                dataStream.nextIntVector(nextBatchSize, dataVector, isNullVector);
            }
        }

        if (inDictionaryStream == null) {
            Arrays.fill(inDictionary, true);
        }
        else {
            inDictionaryStream.getSetBits(nextBatchSize, inDictionary, isNullVector);
        }

        if (useSharedByteArrayForDictionary) {
            // alternative 3 (as described below): make a copy upon read
            int startIndex = 0;
            while (startIndex < nextBatchSize) {
                int totalLength = 0;
                int endIndex = startIndex;
                while (endIndex < nextBatchSize) {
                    int newTotalLength;
                    if (!isNullVector[endIndex]) {
                        if (inDictionary[endIndex]) {
                            newTotalLength = totalLength + dictionary[dataVector[endIndex]].length();
                        }
                        else {
                            newTotalLength = totalLength + rowGroupDictionary[dataVector[endIndex]].length();
                        }
                    }
                    else {
                        newTotalLength = totalLength;
                    }
                    if (newTotalLength > CHUNK_BYTES && totalLength != 0) {
                        // When totalLength = 0 and newTotalLength > CHUNK_BYTES, it means a single element is larger than CHUNK_BYTES.
                        // Calling `break` here will result in infinite loop. Instead, a byte array of CHUNK_BYTES should be allocated.
                        break;
                    }
                    totalLength = newTotalLength;
                    endIndex++;
                }
                byte[] data = new byte[totalLength];
                int bytesCopied = 0;
                for (int i = startIndex; i < endIndex; i++) {
                    if (isNullVector[i]) {
                        sliceVector.vector[i] = null;
                    }
                    else {
                        Slice slice;
                        if (inDictionary[i]) {
                            slice = dictionary[dataVector[i]];
                        }
                        else {
                            slice = rowGroupDictionary[dataVector[i]];
                        }
                        slice.getBytes(0, data, bytesCopied, slice.length());
                        sliceVector.vector[i] = Slices.wrappedBuffer(data, bytesCopied, slice.length());
                        bytesCopied += slice.length();
                    }
                }
                checkState(bytesCopied == totalLength);

                startIndex = endIndex;
            }
        }
        else {
            // alternative 1 or 2 (as described below): reference dictionary directly
            for (int i = 0; i < nextBatchSize; i++) {
                if (isNullVector[i]) {
                    sliceVector.vector[i] = null;
                }
                else if (inDictionary[i]) {
                    sliceVector.vector[i] = dictionary[dataVector[i]];
                }
                else {
                    sliceVector.vector[i] = rowGroupDictionary[dataVector[i]];
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
    }

    private void openRowGroup()
            throws IOException
    {
        // read the dictionary
        if (!dictionaryOpen && dictionarySize > 0) {
            // resize the dictionary array if necessary
            if (dictionary.length < dictionarySize) {
                dictionary = new Slice[dictionarySize];
                dictionaryLength = new int[dictionarySize];
            }

            // read the lengths
            LongStream lengthStream = dictionaryLengthStreamSource.openStream();
            if (lengthStream == null) {
                throw new OrcCorruptionException("Dictionary is not empty but dictionary length stream is not present");
            }
            lengthStream.nextIntVector(dictionarySize, dictionaryLength);

            ByteArrayStream dictionaryDataStream = dictionaryDataStreamSource.openStream();
            readDictionary(dictionaryDataStream, dictionarySize, dictionaryLength, dictionary, useSharedByteArrayForDictionary);
        }
        dictionaryOpen = true;

        // read row group dictionary
        RowGroupDictionaryLengthStream dictionaryLengthStream = rowGroupDictionaryLengthStreamSource.openStream();
        if (dictionaryLengthStream != null) {
            int rowGroupDictionarySize = dictionaryLengthStream.getEntryCount();

            // resize the dictionary array if necessary
            if (rowGroupDictionary.length < rowGroupDictionarySize) {
                rowGroupDictionary = new Slice[rowGroupDictionarySize];
                rowGroupDictionaryLength = new int[rowGroupDictionarySize];
            }

            // read the lengths
            dictionaryLengthStream.nextIntVector(rowGroupDictionarySize, rowGroupDictionaryLength);

            ByteArrayStream dictionaryDataStream = rowGroupDictionaryDataStreamSource.openStream();
            readDictionary(dictionaryDataStream, rowGroupDictionarySize, rowGroupDictionaryLength, rowGroupDictionary, useSharedByteArrayForDictionary);
        }
        dictionaryOpen = true;

        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    private static void readDictionary(@Nullable ByteArrayStream dictionaryDataStream, int dictionarySize, int[] dictionaryLength, Slice[] dictionary, boolean useSharedByteArray)
            throws IOException
    {
        if (useSharedByteArray) {
            int startIndex = 0;
            while (startIndex < dictionarySize) {
                // sum lengths up to CHUNK_BYTES
                int totalLength = 0;
                int endIndex = startIndex;
                while (endIndex < dictionarySize) {
                    int newTotalLength = totalLength + dictionaryLength[endIndex];
                    if (newTotalLength > CHUNK_BYTES && totalLength != 0) {
                        // When totalLength = 0 and newTotalLength > CHUNK_BYTES, it means a single element is larger than CHUNK_BYTES.
                        // Calling `break` here will result in infinite loop. Instead, a byte array of CHUNK_BYTES should be allocated.
                        break;
                    }
                    totalLength = newTotalLength;
                    endIndex++;
                }

                // read dictionary data
                byte[] dictionaryData;
                if (totalLength == 0) {
                    dictionaryData = new byte[0];
                }
                else {
                    if (dictionaryDataStream == null) {
                        throw new OrcCorruptionException("Dictionary length is not zero but dictionary data stream is not present");
                    }
                    dictionaryData = dictionaryDataStream.next(totalLength);
                }

                // build dictionary slices
                int bytesRead = 0;
                for (int i = startIndex; i < endIndex; i++) {
                    int length = dictionaryLength[i];
                    dictionary[i] = Slices.wrappedBuffer(dictionaryData, bytesRead, length);
                    bytesRead += length;
                }
                checkState(bytesRead == totalLength);

                startIndex = endIndex;
            }
        }
        else {
            // build dictionary slices
            for (int i = 0; i < dictionarySize; i++) {
                int length = dictionaryLength[i];
                if (length == 0) {
                    dictionary[i] = Slices.EMPTY_SLICE;
                }
                else {
                    if (dictionaryDataStream == null) {
                        throw new OrcCorruptionException("Dictionary length is not zero but dictionary data stream is not present");
                    }
                    dictionary[i] = Slices.wrappedBuffer(dictionaryDataStream.next(length));
                }
            }
        }
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding, long numberOfRows)
            throws IOException
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStream.class);
        dictionaryLengthStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);
        dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        dictionaryOpen = false;

        // There are 2 alternatives for allocating memories for slices in the dictionary:
        //   (1) Use an individual byte array for each slice. This can cause excessive object allocation, which causes gc issues.
        //   (2) Use a single shared byte array (or a few 8MB chunks) for the whole dictionary. This can cause heap fragmentation and excessive memory retention.
        //   (3) On top of (2), instead of referencing bytes in the shared array, make a copy in a separate byte array.
        // Here, a heuristic is used.
        //   * If numberOfRow / dictionarySize > 5, alternative 1 is used. Excessive object allocation is unlikely to happen because the number of slices in the dictionary is limited.
        //   * Otherwise, alternative 3 is used. This allocates a lot more objects than 1 or 2, but potentially can have a lower pressure on GC. This does not suffer from
        //     excessive memory retention as 2 would.
        // A possibly better alternatives is a variant of alternative 2. Instead of having each data link to the shared buffer, data is copied. This variation reduces memory
        // retention. But implementing it requires a lot more change to the code. It is not implemented.
        useSharedByteArrayForDictionary = dictionarySize == 0 || numberOfRows / dictionarySize <= 5;

        presentStreamSource = missingStreamSource(BooleanStream.class);
        dataStreamSource = missingStreamSource(LongStream.class);

        inDictionaryStreamSource = missingStreamSource(BooleanStream.class);
        rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthStream.class);
        rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class);

        // the "in dictionary" stream signals if the value is in the stripe or row group dictionary
        inDictionaryStreamSource = dataStreamSources.getStreamSource(streamDescriptor, IN_DICTIONARY, BooleanStream.class);
        rowGroupDictionaryLengthStreamSource = dataStreamSources.getStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY_LENGTH, RowGroupDictionaryLengthStream.class);
        rowGroupDictionaryDataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY, ByteArrayStream.class);

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
