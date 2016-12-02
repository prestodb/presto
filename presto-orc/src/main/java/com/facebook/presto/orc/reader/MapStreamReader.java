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
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.joda.time.DateTimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MapStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private final StreamReader keyStreamReader;
    private final StreamReader valueStreamReader;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<LongStream> lengthStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream lengthStream;

    private boolean rowGroupOpen;

    public MapStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.keyStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone);
        this.valueStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(1), hiveStorageTimeZone);
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
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                long entrySkipSize = lengthStream.sum(readOffset);
                keyStreamReader.prepareNextRead(Ints.checkedCast(entrySkipSize));
                valueStreamReader.prepareNextRead(Ints.checkedCast(entrySkipSize));
            }
        }

        // The length vector could be reused, but this simplifies the code below by
        // taking advantage of null entries being initialized to zero.  The vector
        // could be reinitialized for each loop, but that is likely just as expensive
        // as allocating a new array
        int[] lengthVector = new int[nextBatchSize];
        boolean[] nullVector = new boolean[nextBatchSize];
        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException("Value is not null but data stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, lengthVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, lengthVector, nullVector);
            }
        }

        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);

        int entryCount = 0;
        for (int length : lengthVector) {
            entryCount += length;
        }

        Block keys;
        Block values;
        if (entryCount > 0) {
            keyStreamReader.prepareNextRead(entryCount);
            valueStreamReader.prepareNextRead(entryCount);
            keys = keyStreamReader.readBlock(keyType);
            values = valueStreamReader.readBlock(valueType);
        }
        else {
            keys = keyType.createBlockBuilder(new BlockBuilderStatus(), 0).build();
            values = valueType.createBlockBuilder(new BlockBuilderStatus(), 1).build();
        }

        InterleavedBlock keyValueBlock = createKeyValueBlock(nextBatchSize, keys, values, lengthVector);

        // convert lengths into offsets into the keyValueBlock (e.g., two positions per entry)
        int[] offsets = new int[nextBatchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            int length = lengthVector[i - 1] * 2;
            offsets[i] = offsets[i - 1] + length;
        }
        ArrayBlock arrayBlock = new ArrayBlock(nextBatchSize, nullVector, offsets, keyValueBlock);

        readOffset = 0;
        nextBatchSize = 0;

        return arrayBlock;
    }

    private static InterleavedBlock createKeyValueBlock(int positionCount, Block keys, Block values, int[] lengths)
    {
        if (!hasNull(keys)) {
            return new InterleavedBlock(new Block[] {keys, values});
        }

        //
        // Map entries with a null key are skipped in the Hive ORC reader, so skip them here also
        //

        IntArrayList nonNullPositions = new IntArrayList(keys.getPositionCount());

        int position = 0;
        for (int mapIndex = 0; mapIndex < positionCount; mapIndex++) {
            int length = lengths[mapIndex];
            for (int entryIndex = 0; entryIndex < length; entryIndex++) {
                if (keys.isNull(position)) {
                    // key is null, so remove this entry from the map
                    lengths[mapIndex]--;
                }
                else {
                    nonNullPositions.add(position);
                }
                position++;
            }
        }

        Block newKeys = keys.copyPositions(nonNullPositions);
        Block newValues = values.copyPositions(nonNullPositions);
        return new InterleavedBlock(new Block[] {newKeys, newValues});
    }

    private static boolean hasNull(Block keys)
    {
        for (int position = 0; position < keys.getPositionCount(); position++) {
            if (keys.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        lengthStreamSource = missingStreamSource(LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyStreamReader.startStripe(dictionaryStreamSources, encoding);
        valueStreamReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        lengthStreamSource = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyStreamReader.startRowGroup(dataStreamSources);
        valueStreamReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
