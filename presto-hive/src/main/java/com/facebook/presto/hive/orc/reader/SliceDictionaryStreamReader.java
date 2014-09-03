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
package com.facebook.presto.hive.orc.reader;

import com.facebook.presto.hive.orc.SliceVector;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.Vector;
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.ByteArrayStream;
import com.facebook.presto.hive.orc.stream.ByteArrayStreamSource;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.LENGTH;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;

public class SliceDictionaryStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int skipSize;
    private int nextBatchSize;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;
    private final boolean[] isNullVector = new boolean[Vector.MAX_VECTOR_LENGTH];

    private ByteArrayStreamSource dictionaryDataStreamSource;
    private int dictionarySize;
    private Slice[] dictionary = new Slice[0];

    private LongStreamSource dictionaryLengthStreamSource;
    private int[] dictionaryLength = new int[0];

    private LongStreamSource dataStreamSource;
    private LongStream dataStream;
    private final int[] dataVector = new int[Vector.MAX_VECTOR_LENGTH];

    public SliceDictionaryStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void setNextBatchSize(int batchSize)
    {
        skipSize += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (dataStream == null) {
            openStreams();
        }

        if (skipSize != 0) {
            if (presentStreamSource != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                skipSize = presentStream.countBitsSet(skipSize);
            }
            dataStream.skip(skipSize);
        }

        SliceVector sliceVector = (SliceVector) vector;

        if (presentStream == null) {
            Arrays.fill(isNullVector, false);
            dataStream.nextIntVector(nextBatchSize, dataVector);
        }
        else {
            presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (dataStream != null) {
                dataStream.nextIntVector(nextBatchSize, dataVector, isNullVector);
            }
            else {
                // make sure there are no non null values
                for (int i = 0; i < nextBatchSize; i++) {
                    if (!isNullVector[i]) {
                        throw new IllegalStateException("Value is not null, but data stream is not present");
                    }
                }
            }
        }

        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                sliceVector.slice[i] = dictionary[dataVector[i]];
            }
            else {
                sliceVector.slice[i] = null;
            }
        }

        skipSize = 0;
        nextBatchSize = 0;
    }

    private void openStreams()
            throws IOException
    {
        // read the dictionary
        if (dictionaryDataStreamSource != null) {
            // initialize offset and length arrays
            if (dictionary.length < dictionarySize) {
                dictionary = new Slice[dictionarySize];
                dictionaryLength = new int[dictionarySize];
            }

            // read the lengths
            LongStream lengthIntegerReader = dictionaryLengthStreamSource.openStream();
            lengthIntegerReader.nextIntVector(dictionarySize, dictionaryLength);

            // sum lengths
            int totalLength = 0;
            for (int i = 0; i < dictionarySize; i++) {
                totalLength += dictionaryLength[i];
            }

            // read dictionary data
            ByteArrayStream dictionaryByteArrayStream = dictionaryDataStreamSource.openStream();
            byte[] dictionaryData = dictionaryByteArrayStream.next(totalLength);

            // build dictionary slices
            int offset = 0;
            for (int i = 0; i < dictionarySize; i++) {
                int length = dictionaryLength[i];
                dictionary[i] = Slices.wrappedBuffer(dictionaryData, offset, length);
                offset += length;
            }
        }

        // open present stream
        if (presentStreamSource != null) {
            if (presentStream == null) {
                presentStream = presentStreamSource.openStream();
            }
        }

        // open data stream
        if (dataStreamSource != null) {
            dataStream = dataStreamSource.openStream();
        }
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStreamSource.class);
        dictionaryLengthStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStreamSource.class);
        presentStreamSource = null;
        dataStreamSource = null;

        dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        dictionaryLength = new int[0];
        dictionary = new Slice[0];
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        dataStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, DATA, LongStreamSource.class);

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
