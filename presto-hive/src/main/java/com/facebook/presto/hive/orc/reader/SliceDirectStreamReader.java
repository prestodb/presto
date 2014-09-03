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
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.LENGTH;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;

public class SliceDirectStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int skipSize;
    private int nextBatchSize;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;
    private final boolean[] isNullVector = new boolean[Vector.MAX_VECTOR_LENGTH];

    private LongStreamSource lengthStreamSource;
    private LongStream lengthStream;
    private final int[] lengthVector = new int[Vector.MAX_VECTOR_LENGTH];

    private ByteArrayStreamSource dataByteSource;
    private ByteArrayStream dataStream;

    public SliceDirectStreamReader(StreamDescriptor streamDescriptor)
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
        if (presentStream == null && lengthStream == null) {
            openStreams();
        }

        if (skipSize != 0) {
            if (presentStreamSource != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                skipSize = presentStream.countBitsSet(skipSize);
            }
            long dataSkipSize = lengthStream.sum(skipSize);
            dataStream.skip(dataSkipSize);
        }

        SliceVector sliceVector = (SliceVector) vector;
        if (presentStream == null) {
            lengthStream.nextIntVector(nextBatchSize, lengthVector);
        }
        else {
            presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (lengthStream != null) {
                lengthStream.nextIntVector(nextBatchSize, lengthVector, isNullVector);
            }
        }

        int totalLength = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                totalLength += lengthVector[i];
            }
        }

        byte[] data = dataStream.next(totalLength);

        int offset = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                int length = lengthVector[i];
                sliceVector.slice[i] = Slices.wrappedBuffer(data, offset, length);
                offset += length;
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
        if (presentStreamSource != null) {
            if (presentStream == null) {
                presentStream = presentStreamSource.openStream();
            }
        }

        if (lengthStreamSource != null) {
            lengthStream = lengthStreamSource.openStream();
        }

        dataStream = dataByteSource.openStream();
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = null;
        lengthStreamSource = null;
        dataByteSource = null;

        skipSize = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
        lengthStream = null;
        dataStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        lengthStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, LENGTH, LongStreamSource.class);
        dataByteSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, DATA, ByteArrayStreamSource.class);

        skipSize = 0;
        nextBatchSize = 0;

        if (presentStreamSource == null) {
            Arrays.fill(isNullVector, false);
        }
        presentStream = null;
        lengthStream = null;
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
