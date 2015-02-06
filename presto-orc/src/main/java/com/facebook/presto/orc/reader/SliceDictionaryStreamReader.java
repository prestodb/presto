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

public class SliceDictionaryStreamReader
        implements StreamReader
{
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
            readDictionary(dictionaryDataStream, dictionarySize, dictionaryLength, dictionary);
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
            readDictionary(dictionaryDataStream, rowGroupDictionarySize, rowGroupDictionaryLength, rowGroupDictionary);
        }
        dictionaryOpen = true;

        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    private static void readDictionary(@Nullable ByteArrayStream dictionaryDataStream, int dictionarySize, int[] dictionaryLength, Slice[] dictionary)
            throws IOException
    {
        // build dictionary slices
        for (int i = 0; i < dictionarySize; i++) {
            int length = dictionaryLength[i];
            if (length == 0) {
                dictionary[i] = Slices.EMPTY_SLICE;
            }
            else {
                dictionary[i] = Slices.wrappedBuffer(dictionaryDataStream.next(length));
            }
        }
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStream.class);
        dictionaryLengthStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);
        dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        dictionaryOpen = false;

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
