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
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.ByteArrayStream;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.StreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.orc.OrcCorruptionException.verifyFormat;
import static com.facebook.presto.hive.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DICTIONARY_DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

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
    private Slice[] dictionary = new Slice[0];

    @Nonnull
    private StreamSource<LongStream> dictionaryLengthStreamSource = missingStreamSource(LongStream.class);
    private int[] dictionaryLength = new int[0];

    @Nonnull
    private StreamSource<LongStream> dataStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream dataStream;
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
                verifyFormat(dataStream != null, "Value is not null but data stream is not present");
                dataStream.skip(readOffset);
            }
        }

        SliceVector sliceVector = (SliceVector) vector;

        if (presentStream == null) {
            verifyFormat(dataStream != null, "Value is not null but data stream is not present");
            Arrays.fill(isNullVector, false);
            dataStream.nextIntVector(nextBatchSize, dataVector);
        }
        else {
            int nonNullValues = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nonNullValues != nextBatchSize) {
                verifyFormat(dataStream != null, "Value is not null but data stream is not present");
                dataStream.nextIntVector(nextBatchSize, dataVector, isNullVector);
            }
        }

        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                sliceVector.vector[i] = dictionary[dataVector[i]];
            }
            else {
                sliceVector.vector[i] = null;
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
            // initialize offset and length arrays
            if (dictionary.length < dictionarySize) {
                dictionary = new Slice[dictionarySize];
                dictionaryLength = new int[dictionarySize];
            }

            // read the lengths
            LongStream lengthStream = dictionaryLengthStreamSource.openStream();
            verifyFormat(lengthStream != null, "Dictionary is not empty but dictionary length stream is not present");
            lengthStream.nextIntVector(dictionarySize, dictionaryLength);

            // sum lengths
            int totalLength = 0;
            for (int i = 0; i < dictionarySize; i++) {
                totalLength += dictionaryLength[i];
            }

            // read dictionary data
            byte[] dictionaryData = new byte[0];
            if (totalLength > 0) {
                ByteArrayStream dictionaryByteArrayStream = dictionaryDataStreamSource.openStream();
                verifyFormat(dictionaryByteArrayStream != null, "Dictionary length is not zero but dictionary data stream is not present");
                dictionaryData = dictionaryByteArrayStream.next(totalLength);
            }

            // build dictionary slices
            int offset = 0;
            for (int i = 0; i < dictionarySize; i++) {
                int length = dictionaryLength[i];
                dictionary[i] = Slices.wrappedBuffer(dictionaryData, offset, length);
                offset += length;
            }
        }
        dictionaryOpen = true;

        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
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

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        dictionaryLength = new int[0];
        dictionary = new Slice[0];

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
