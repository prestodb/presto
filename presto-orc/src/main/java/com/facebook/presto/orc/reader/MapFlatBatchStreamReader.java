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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_MAP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.ReaderUtils.verifyStreamType;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Flat Maps are a layout of maps supported in DWRF.
 * <p>
 * Values associated with different keys are stored in separate streams rather than having a single set of value streams for the map.
 * <p>
 * There is a ColumnEncoding associated with the value streams for a given key.  All the ColumnEncodings for values have the same
 * columnId, and use a sequenceId to distinguish them.  The ColumnEncoding also contains the key it is associated with as metadata.
 * <p>
 * Note that the ColumnEncoding with sequenceId 0 for the values has no data associated with it, only statistics.  Similarly there
 * is a ColumnEncoding for the key stream which has no data associated with it, only statistics, so it is not used in this class.
 */
public class MapFlatBatchStreamReader
        implements BatchStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapFlatBatchStreamReader.class).instanceSize();

    private final MapType type;
    private final StreamDescriptor streamDescriptor;
    private final DateTimeZone hiveStorageTimeZone;

    // This is the StreamDescriptor for the value stream with sequence ID 0, it is used to derive StreamDescriptors for the
    // value streams with other sequence IDs
    private final StreamDescriptor baseValueStreamDescriptor;
    private final OrcType.OrcTypeKind keyOrcType;

    private final List<InputStreamSource<BooleanInputStream>> inMapStreamSources = new ArrayList<>();
    private final List<BooleanInputStream> inMapStreams = new ArrayList<>();
    private final List<BatchStreamReader> valueStreamReaders = new ArrayList<>();
    private final List<StreamDescriptor> valueStreamDescriptors = new ArrayList<>();

    private Block keyBlockTemplate;
    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private boolean rowGroupOpen;

    private OrcAggregatedMemoryContext systemMemoryContext;
    private final OrcRecordReaderOptions options;

    public MapFlatBatchStreamReader(Type type, StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, OrcRecordReaderOptions options, OrcAggregatedMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(streamDescriptor, type, MapType.class::isInstance);
        this.type = (MapType) type;
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.keyOrcType = streamDescriptor.getNestedStreams().get(0).getOrcTypeKind();
        this.baseValueStreamDescriptor = streamDescriptor.getNestedStreams().get(1);
        this.options = requireNonNull(options);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock()
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
                for (int i = 0; i < valueStreamReaders.size(); i++) {
                    int valueReadOffset = inMapStreams.get(i).countBitsSet(readOffset);
                    valueStreamReaders.get(i).prepareNextRead(valueReadOffset);
                }
            }
        }

        boolean[][] inMapVectors = new boolean[inMapStreamSources.size()][];
        boolean[] nullVector = null;
        int totalMapEntries = 0;

        if (presentStream == null) {
            for (int keyIndex = 0; keyIndex < inMapStreams.size(); keyIndex++) {
                inMapVectors[keyIndex] = new boolean[nextBatchSize];
                totalMapEntries += inMapStreams.get(keyIndex).getSetBits(nextBatchSize, inMapVectors[keyIndex]);
            }
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                for (int i = 0; i < inMapStreams.size(); i++) {
                    inMapVectors[i] = new boolean[nextBatchSize];
                    totalMapEntries += inMapStreams.get(i).getSetBits(nextBatchSize, inMapVectors[i], nullVector);
                }
            }
        }

        MapType mapType = (MapType) type;
        Type valueType = mapType.getValueType();

        Block[] valueBlocks = new Block[valueStreamReaders.size()];

        if (totalMapEntries > 0) {
            for (int keyIndex = 0; keyIndex < valueStreamReaders.size(); keyIndex++) {
                int mapsContainingKey = 0;

                for (int mapIndex = 0; mapIndex < nextBatchSize; mapIndex++) {
                    if (inMapVectors[keyIndex][mapIndex]) {
                        mapsContainingKey++;
                    }
                }

                if (mapsContainingKey > 0) {
                    BatchStreamReader streamReader = valueStreamReaders.get(keyIndex);
                    streamReader.prepareNextRead(mapsContainingKey);
                    valueBlocks[keyIndex] = streamReader.readBlock();
                }
                else {
                    valueBlocks[keyIndex] = valueType.createBlockBuilder(null, 0).build();
                }
            }
        }

        int[] valueBlockPositions = new int[inMapVectors.length];
        BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, totalMapEntries);
        int[] keyIds = new int[totalMapEntries];
        int keyIdsIndex = 0;
        int[] mapOffsets = new int[nextBatchSize + 1];
        mapOffsets[0] = 0;

        for (int mapIndex = 0; mapIndex < nextBatchSize; mapIndex++) {
            int mapLength = 0;
            if (totalMapEntries > 0) {
                for (int keyIndex = 0; keyIndex < inMapVectors.length; keyIndex++) {
                    if (inMapVectors[keyIndex][mapIndex]) {
                        mapLength++;
                        valueType.appendTo(valueBlocks[keyIndex], valueBlockPositions[keyIndex], valueBlockBuilder);
                        keyIds[keyIdsIndex++] = keyIndex;
                        valueBlockPositions[keyIndex]++;
                    }
                }
            }

            mapOffsets[mapIndex + 1] = mapOffsets[mapIndex] + mapLength;
        }

        Block block = mapType.createBlockFromKeyValue(nextBatchSize, Optional.ofNullable(nullVector), mapOffsets, new DictionaryBlock(keyBlockTemplate, keyIds), valueBlockBuilder);

        readOffset = 0;
        nextBatchSize = 0;

        return block;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();

        for (int i = 0; i < inMapStreamSources.size(); i++) {
            BooleanInputStream inMapStream = requireNonNull(inMapStreamSources.get(i).openStream(), "missing inMapStream at position " + i);
            inMapStreams.add(inMapStream);
        }

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(Stripe stripe)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);

        inMapStreamSources.clear();
        valueStreamDescriptors.clear();
        valueStreamReaders.clear();

        ColumnEncoding encoding = stripe.getColumnEncodings().get(baseValueStreamDescriptor.getStreamId());
        SortedMap<Integer, DwrfSequenceEncoding> additionalSequenceEncodings = Collections.emptySortedMap();
        // encoding or encoding.getAdditionalSequenceEncodings() may not be present when every map is empty or null
        if (encoding != null && encoding.getAdditionalSequenceEncodings().isPresent()) {
            additionalSequenceEncodings = encoding.getAdditionalSequenceEncodings().get();
        }

        // The ColumnEncoding with sequence ID 0 doesn't have any data associated with it
        for (int sequence : additionalSequenceEncodings.keySet()) {
            inMapStreamSources.add(missingStreamSource(BooleanInputStream.class));

            StreamDescriptor valueStreamDescriptor = copyStreamDescriptorWithSequence(baseValueStreamDescriptor, sequence);
            valueStreamDescriptors.add(valueStreamDescriptor);

            BatchStreamReader valueStreamReader = BatchStreamReaders.createStreamReader(type.getValueType(), valueStreamDescriptor, hiveStorageTimeZone, options, systemMemoryContext);
            valueStreamReader.startStripe(stripe);
            valueStreamReaders.add(valueStreamReader);
        }

        keyBlockTemplate = getKeyBlockTemplate(additionalSequenceEncodings.values());
        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;
    }

    /**
     * Creates StreamDescriptor which is a copy of this one with the value of sequence changed to
     * the value passed in.  Recursively calls itself on the nested streams.
     */
    private static StreamDescriptor copyStreamDescriptorWithSequence(StreamDescriptor streamDescriptor, int sequence)
    {
        List<StreamDescriptor> streamDescriptors = streamDescriptor.getNestedStreams().stream()
                .map(stream -> copyStreamDescriptorWithSequence(stream, sequence))
                .collect(toImmutableList());

        return new StreamDescriptor(
                streamDescriptor.getStreamName(),
                streamDescriptor.getStreamId(),
                streamDescriptor.getFieldName(),
                streamDescriptor.getOrcType(),
                streamDescriptor.getOrcDataSource(),
                streamDescriptors,
                sequence);
    }

    private Block getKeyBlockTemplate(Collection<DwrfSequenceEncoding> sequenceEncodings)
    {
        switch (keyOrcType) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return getIntegerKeyBlockTemplate(sequenceEncodings);
            case STRING:
            case BINARY:
                return getSliceKeysBlockTemplate(sequenceEncodings);
            default:
                throw new IllegalArgumentException("Unsupported flat map key type: " + keyOrcType);
        }
    }

    private Block getIntegerKeyBlockTemplate(Collection<DwrfSequenceEncoding> sequenceEncodings)
    {
        Type keyType;

        switch (keyOrcType) {
            case BYTE:
                keyType = TinyintType.TINYINT;
                break;
            case SHORT:
                keyType = SmallintType.SMALLINT;
                break;
            case INT:
                keyType = IntegerType.INTEGER;
                break;
            case LONG:
                keyType = BigintType.BIGINT;
                break;
            default:
                throw new IllegalArgumentException("Unsupported flat map key type: " + keyOrcType);
        }

        BlockBuilder blockBuilder = keyType.createBlockBuilder(null, sequenceEncodings.size());

        for (DwrfSequenceEncoding sequenceEncoding : sequenceEncodings) {
            keyType.writeLong(blockBuilder, sequenceEncoding.getKey().getIntKey());
        }

        return blockBuilder.build();
    }

    private Block getSliceKeysBlockTemplate(Collection<DwrfSequenceEncoding> sequenceEncodings)
    {
        int bytes = 0;

        for (DwrfSequenceEncoding sequenceEncoding : sequenceEncodings) {
            bytes += sequenceEncoding.getKey().getBytesKey().size();
        }

        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, sequenceEncodings.size(), bytes);

        for (DwrfSequenceEncoding sequenceEncoding : sequenceEncodings) {
            Slice key = Slices.wrappedBuffer(sequenceEncoding.getKey().getBytesKey().toByteArray());
            builder.writeBytes(key, 0, key.length());
            builder.closeEntry();
        }

        return builder.build();
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);

        for (int i = 0; i < valueStreamDescriptors.size(); i++) {
            InputStreamSource<BooleanInputStream> inMapStreamSource = dataStreamSources.getInputStreamSource(valueStreamDescriptors.get(i), IN_MAP, BooleanInputStream.class);
            inMapStreamSources.set(i, inMapStreamSource);
        }

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inMapStreams.clear();

        rowGroupOpen = false;

        for (BatchStreamReader valueStreamReader : valueStreamReaders) {
            valueStreamReader.startRowGroup(dataStreamSources);
        }
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
            for (BatchStreamReader valueStreamReader : valueStreamReaders) {
                closer.register(valueStreamReader::close);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSize = INSTANCE_SIZE;

        for (BatchStreamReader valueStreamReader : valueStreamReaders) {
            retainedSize += valueStreamReader.getRetainedSizeInBytes();
        }

        return retainedSize;
    }
}
