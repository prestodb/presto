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
package com.facebook.presto.orc.block;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.ByteArrayStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class SliceDirectBlockReader
        implements BlockReader
{
    private final StreamDescriptor streamDescriptor;

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private LongStream lengthStream;

    @Nullable
    private ByteArrayStream dataStream;

    @Nonnull
    private byte[] data = new byte[1024];

    public SliceDirectBlockReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public boolean readNextValueInto(BlockBuilder builder, boolean skipNull)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            if (!skipNull) {
                builder.appendNull();
                return true;
            }
            return false;
        }

        int length = bufferNextValue();

        VARCHAR.writeSlice(builder, Slices.wrappedBuffer(data, 0, length));
        return true;
    }

    private int bufferNextValue()
            throws IOException
    {
        if (lengthStream == null) {
            throw new OrcCorruptionException("Value is not null but length stream is not present");
        }

        int length = Ints.checkedCast(lengthStream.next());
        if (data.length < length) {
            data = new byte[length];
        }

        if (length > 0) {
            if (dataStream == null) {
                throw new OrcCorruptionException("Length is not zero but data stream is not present");
            }
            dataStream.next(length, data);
        }
        return length;
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        if (skipSize == 0) {
            return;
        }

        if (lengthStream == null) {
            throw new OrcCorruptionException("Value is not null but length stream is not present");
        }

        // skip non-null length
        long dataSkipSize = lengthStream.sum(skipSize);

        if (dataSkipSize == 0) {
            return;
        }

        if (dataStream == null) {
            throw new OrcCorruptionException("Length is not zero but data stream is not present");
        }

        // skip data bytes
        dataStream.skip(Ints.checkedCast(dataSkipSize));
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        lengthStream = null;
        dataStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        lengthStream = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();
        dataStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, ByteArrayStream.class).openStream();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
