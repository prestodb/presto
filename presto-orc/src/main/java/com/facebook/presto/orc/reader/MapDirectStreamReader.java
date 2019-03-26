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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.Subfield.LongSubscript;
import com.facebook.presto.spi.Subfield.PathElement;
import com.facebook.presto.spi.Subfield.StringSubscript;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.Subfield.allSubscripts;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class MapDirectStreamReader
        extends ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapDirectStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final StreamReader keyStreamReader;
    private final StreamReader valueStreamReader;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private Type keyType;
    private Type valueType;
    private Set<Long> longSubscripts;
    private Set<Slice> sliceSubscripts;

    public MapDirectStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        super(OptionalInt.empty());
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.keyStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone, systemMemoryContext);
        this.valueStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(1), hiveStorageTimeZone, systemMemoryContext);
    }

    @Override
    public void setReferencedSubfields(List<Subfield> subfields, int depth)
    {
        boolean mayPruneKey = true;
        boolean mayPruneElement = true;
        ImmutableList.Builder<Subfield> pathsForElement = ImmutableList.builder();
        ImmutableSet.Builder<Slice> sliceSubscripts = ImmutableSet.builder();
        ImmutableSet.Builder<Long> longSubscripts = ImmutableSet.builder();
        for (Subfield subfield : subfields) {
            List<PathElement> pathElements = subfield.getPath();
            PathElement subscript = pathElements.get(depth);
            checkArgument(subscript.isSubscript(), "Map reader needs a PathElement with a subscript");
            if (subscript == allSubscripts()) {
                mayPruneKey = false;
            }
            else {
                if (subscript instanceof StringSubscript) {
                    sliceSubscripts.add(Slices.copiedBuffer(((StringSubscript) subscript).getIndex(), UTF_8));
                }
                else {
                    longSubscripts.add(((LongSubscript) subscript).getIndex());
                }
            }
            if (pathElements.size() > depth + 1) {
                pathsForElement.add(subfield);
            }
            else {
                mayPruneElement = false;
            }
        }
        if (mayPruneElement) {
            valueStreamReader.setReferencedSubfields(pathsForElement.build(), depth + 1);
        }
        if (mayPruneKey) {
            this.sliceSubscripts = sliceSubscripts.build();
            this.longSubscripts = longSubscripts.build();
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
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                long entrySkipSize = lengthStream.sum(readOffset);
                keyStreamReader.prepareNextRead(toIntExact(entrySkipSize));
                valueStreamReader.prepareNextRead(
                        toIntExact(entrySkipSize));
            }
        }

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];
        boolean[] nullVector = null;

        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, offsetVector, 0);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, offsetVector, 0, nullVector);
            }
        }

        MapType mapType = (MapType) type;
        keyType = mapType.getKeyType();
        valueType = mapType.getValueType();

        // Calculate the entryCount. Note that the values in the offsetVector are still length values now.
        int entryCount = 0;
        for (int i = 0; i < offsetVector.length - 1; i++) {
            entryCount += offsetVector[i];
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
            keys = keyType.createBlockBuilder(null, 0).build();
            values = valueType.createBlockBuilder(null, 1).build();
        }

        Block[] keyValueBlock = createKeyValueBlock(nextBatchSize, keys, values, offsetVector);

        // Convert the length values in the offsetVector to offset values in-place
        int currentLength = offsetVector[0];
        offsetVector[0] = 0;
        for (int i = 1; i < offsetVector.length; i++) {
            int lastLength = offsetVector[i];
            offsetVector[i] = offsetVector[i - 1] + currentLength;
            currentLength = lastLength;
        }

        readOffset = 0;
        nextBatchSize = 0;

        return mapType.createBlockFromKeyValue(Optional.ofNullable(nullVector), offsetVector, keyValueBlock[0], keyValueBlock[1]);
    }

    boolean canPruneKeys(Block keys)
    {
        return longSubscripts != null || sliceSubscripts != null;
    }

    private boolean keyIsPruned(Block keys, int position)
    {
        return (longSubscripts != null && !longSubscripts.contains(keyType.getLong(keys, position))) ||
            (sliceSubscripts != null && !sliceSubscripts.contains(keyType.getSlice(keys, position)));
    }

    private Block[] createKeyValueBlock(int positionCount, Block keys, Block values, int[] lengths)
    {
        if (!hasNull(keys) && !canPruneKeys(keys)) {
            return new Block[] {keys, values};
        }

        //
        // Map entries with a null key are skipped in the Hive ORC reader, so skip them here also
        //

        IntArrayList nonNullPositions = new IntArrayList(keys.getPositionCount());

        int position = 0;
        for (int mapIndex = 0; mapIndex < positionCount; mapIndex++) {
            int length = lengths[mapIndex];
            for (int entryIndex = 0; entryIndex < length; entryIndex++) {
                if (keys.isNull(position) || keyIsPruned(keys, position)) {
                    // key is null, so remove this entry from the map
                    lengths[mapIndex]--;
                }
                else {
                    nonNullPositions.add(position);
                }
                position++;
            }
        }

        Block newKeys = keys.copyPositions(nonNullPositions.elements(), 0, nonNullPositions.size());
        Block newValues = values.copyPositions(nonNullPositions.elements(), 0, nonNullPositions.size());
        return new Block[] {newKeys, newValues};
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

    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyStreamReader.startStripe(dictionaryStreamSources, encoding);
        valueStreamReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

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

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(keyStreamReader::close);
            closer.register(valueStreamReader::close);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + keyStreamReader.getRetainedSizeInBytes() + valueStreamReader.getRetainedSizeInBytes();
    }
}
