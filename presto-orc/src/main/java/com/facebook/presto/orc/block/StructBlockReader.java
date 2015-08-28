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

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.block.BlockReaders.createBlockReader;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class StructBlockReader
        implements BlockReader
{
    private final DynamicSliceOutput out = new DynamicSliceOutput(1024);

    private final StreamDescriptor streamDescriptor;
    private final boolean checkForNulls;
    private final BlockReader[] structFields;

    @Nullable
    private BooleanStream presentStream;

    public StructBlockReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone hiveStorageTimeZone, Type type)
    {
        checkNotNull(type, "type is null");

        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.checkForNulls = checkForNulls;

        List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams();
        this.structFields = new BlockReader[nestedStreams.size()];
        for (int i = 0; i < nestedStreams.size(); i++) {
            StreamDescriptor nestedStream = nestedStreams.get(i);
            this.structFields[i] = createBlockReader(nestedStream, true, hiveStorageTimeZone, type.getTypeParameters().get(i));
        }
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

        BlockBuilder currentBuilder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), structFields.length);
        for (BlockReader structField : structFields) {
            structField.readNextValueInto(currentBuilder, false);
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

        // skip non-null values
        for (BlockReader structField : structFields) {
            structField.skip(skipSize);
        }
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;

        for (BlockReader structField : structFields) {
            structField.openStripe(dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        if (checkForNulls) {
            presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        }

        for (BlockReader structField : structFields) {
            structField.openRowGroup(dataStreamSources);
        }
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
