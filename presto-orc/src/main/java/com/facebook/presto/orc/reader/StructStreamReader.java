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

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class StructStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private final StreamReader[] structFields;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    private boolean rowGroupOpen;

    public StructStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");

        List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams();
        this.structFields = new StreamReader[nestedStreams.size()];
        for (int i = 0; i < nestedStreams.size(); i++) {
            StreamDescriptor nestedStream = nestedStreams.get(i);
            this.structFields[i] = createStreamReader(nestedStream, hiveStorageTimeZone);
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the field readers
                readOffset = presentStream.countBitsSet(readOffset);
            }
            for (StreamReader structField : structFields) {
                structField.prepareNextRead(readOffset);
            }
        }

        List<Type> typeParameters = type.getTypeParameters();

        boolean[] nullVector = new boolean[nextBatchSize];
        Block[] blocks = new Block[typeParameters.size()];
        if (presentStream == null) {
            for (int i = 0; i < typeParameters.size(); i++) {
                StreamReader structField = structFields[i];
                structField.prepareNextRead(nextBatchSize);
                blocks[i] = structField.readBlock(typeParameters.get(i));
            }
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                for (int i = 0; i < typeParameters.size(); i++) {
                    StreamReader structField = structFields[i];
                    structField.prepareNextRead(nextBatchSize - nullValues);
                    blocks[i] = structField.readBlock(typeParameters.get(i));
                }
            }
            else {
                for (int i = 0; i < typeParameters.size(); i++) {
                    blocks[i] = typeParameters.get(i).createBlockBuilder(new BlockBuilderStatus(), 0).build();
                }
            }
        }

        // Build offsets for array block (null valued have no positions)
        int[] offsets = new int[nextBatchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            int length = nullVector[i - 1] ? 0 : typeParameters.size();
            offsets[i] = offsets[i - 1] + length;
        }

        // Struct is represented as an array block holding an interleaved block
        InterleavedBlock interleavedBlock = new InterleavedBlock(blocks);
        ArrayBlock arrayBlock = new ArrayBlock(nextBatchSize, nullVector, offsets, interleavedBlock);

        readOffset = 0;
        nextBatchSize = 0;

        return arrayBlock;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (StreamReader structField : structFields) {
            structField.startStripe(dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (StreamReader structField : structFields) {
            structField.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
