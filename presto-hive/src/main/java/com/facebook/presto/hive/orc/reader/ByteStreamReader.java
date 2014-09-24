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

import com.facebook.presto.hive.orc.LongVector;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.ByteStream;
import com.facebook.presto.hive.orc.stream.ByteStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;

public class ByteStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int skipSize;
    private int nextBatchSize;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;

    private ByteStreamSource dataStreamSource;
    private ByteStream dataStream;

    public ByteStreamReader(StreamDescriptor streamDescriptor)
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
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                skipSize = presentStream.countBitsSet(skipSize);
            }
            dataStream.skip(skipSize);
        }

        LongVector byteVector = (LongVector) vector;
        if (presentStream == null) {
            Arrays.fill(byteVector.isNull, false);
            dataStream.nextVector(nextBatchSize, byteVector.vector);
        }
        else {
            presentStream.getUnsetBits(nextBatchSize, byteVector.isNull);
            dataStream.nextVector(nextBatchSize, byteVector.vector, byteVector.isNull);
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

        dataStream = dataStreamSource.openStream();
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = null;
        dataStreamSource = null;

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, ByteStreamSource.class);

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
