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
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class SliceDirectStreamReader
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
    private StreamSource<LongStream> lengthStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream lengthStream;
    private final int[] lengthVector = new int[Vector.MAX_VECTOR_LENGTH];

    @Nonnull
    private StreamSource<ByteArrayStream> dataByteSource = missingStreamSource(ByteArrayStream.class);
    @Nullable
    private ByteArrayStream dataStream;

    private boolean rowGroupOpen;

    public SliceDirectStreamReader(StreamDescriptor streamDescriptor)
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
                verifyFormat(lengthStream != null, "Value is not null but length stream is not present");
                long dataSkipSize = lengthStream.sum(readOffset);
                if (dataSkipSize > 0) {
                    verifyFormat(dataStream != null, "Value is not null but data stream is not present");
                    dataStream.skip(dataSkipSize);
                }
            }
        }

        SliceVector sliceVector = (SliceVector) vector;
        if (presentStream == null) {
            verifyFormat(lengthStream != null, "Value is not null but length stream is not present");
            lengthStream.nextIntVector(nextBatchSize, lengthVector);
        }
        else {
            int nonNullValues = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nonNullValues != nextBatchSize) {
                verifyFormat(lengthStream != null, "Value is not null but length stream is not present");
                lengthStream.nextIntVector(nextBatchSize, lengthVector, isNullVector);
            }
        }

        int totalLength = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                totalLength += lengthVector[i];
            }
        }

        byte[] data = new byte[0];
        if (totalLength > 0) {
            verifyFormat(dataStream != null, "Value is not null but data stream is not present");
            data = dataStream.next(totalLength);
        }

        int offset = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                int length = lengthVector[i];
                sliceVector.vector[i] = Slices.wrappedBuffer(data, offset, length);
                offset += length;
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
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();
        dataStream = dataByteSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        lengthStreamSource = missingStreamSource(LongStream.class);
        dataByteSource = missingStreamSource(ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        lengthStreamSource = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);
        dataByteSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
        lengthStream = null;
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
