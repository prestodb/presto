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
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.block.BlockReaders.createBlockReader;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class MapBlockReader
        implements BlockReader
{
    private final DynamicSliceOutput out = new DynamicSliceOutput(1024);

    private final StreamDescriptor streamDescriptor;
    private final boolean checkForNulls;

    private final BlockReader keyReader;
    private final BlockReader valueReader;

    @Nullable
    private BooleanStream presentStream;
    @Nullable
    private LongStream lengthStream;

    public MapBlockReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone hiveStorageTimeZone, Type type)
    {
        checkNotNull(type, "type is null");

        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.checkForNulls = checkForNulls;

        keyReader = createBlockReader(streamDescriptor.getNestedStreams().get(0), true, hiveStorageTimeZone, type.getTypeParameters().get(0));
        valueReader = createBlockReader(streamDescriptor.getNestedStreams().get(1), true, hiveStorageTimeZone, type.getTypeParameters().get(1));
    }

    @Override
    public boolean readNextValueInto(BlockBuilder builder, boolean skipNull)
            throws IOException
    {
        out.reset();

        if (presentStream != null && !presentStream.nextBit()) {
            if (!skipNull) {
                checkNotNull(builder, "parent builder is null").appendNull();
                return true;
            }
            return false;
        }

        if (lengthStream == null) {
            throw new OrcCorruptionException("Value is not null but length stream is not present");
        }

        long length = lengthStream.next();
        BlockBuilder currentBuilder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), Ints.checkedCast(length));
        for (int i = 0; i < length; i++) {
            if (keyReader.readNextValueInto(currentBuilder, true)) {
                valueReader.readNextValueInto(currentBuilder, false);
            }
            else {
                valueReader.skip(1);
            }
        }

        currentBuilder.getEncoding().writeBlock(out, currentBuilder.build());

        if (builder != null) {
            VARBINARY.writeSlice(builder, out.copySlice());
        }
        return true;
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        if (skipSize == 0)  {
            return;
        }

        if (lengthStream == null) {
            throw new OrcCorruptionException("Value is not null but length stream is not present");
        }

        // skip non-null values
        long elementSkipSize = lengthStream.sum(skipSize);
        keyReader.skip(Ints.checkedCast(elementSkipSize));
        valueReader.skip(Ints.checkedCast(elementSkipSize));
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        lengthStream = null;

        keyReader.openStripe(dictionaryStreamSources, encoding);
        valueReader.openStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        if (checkForNulls) {
            presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        }
        lengthStream = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();

        keyReader.openRowGroup(dataStreamSources);
        valueReader.openRowGroup(dataStreamSources);
    }

    @Override
    public Slice toSlice()
    {
        return out.copySlice();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
