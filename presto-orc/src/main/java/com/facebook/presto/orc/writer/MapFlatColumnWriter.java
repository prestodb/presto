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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.MapColumnStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsBuilder;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.MEDIUM;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static com.facebook.presto.orc.writer.ColumnWriterUtils.buildRowGroupIndexes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MapFlatColumnWriter
        implements ColumnWriter
{
    // sequence must start from 1 because presto-orc treats sequence 0 the same as a missing sequence
    private static final int SEQUENCE_START_INDEX = 1;
    private static final ColumnEncoding FLAT_MAP_COLUMN_ENCODING = new ColumnEncoding(DWRF_MAP_FLAT, 0);
    private static final DwrfProto.KeyInfo EMPTY_SLICE_KEY = DwrfProto.KeyInfo.newBuilder().setBytesKey(ByteString.EMPTY).build();
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapFlatColumnWriter.class).instanceSize();
    private static final int MIN_CAPACITY = 16;

    private final int nodeIndex;
    private final int keyNodeIndex;
    private final int valueNodeIndex;
    private final Type keyType;
    private final ColumnWriterOptions columnWriterOptions;
    private final Optional<DwrfDataEncryptor> dwrfEncryptor;
    private final boolean compressed;
    private final PresentOutputStream presentStream;
    private final CompressedMetadataWriter metadataWriter;
    private final KeyManager keyManager;
    private final int maxFlattenedMapKeyCount;
    private final boolean mapStatsEnabled;

    // Pre-create a value block with a single null value to avoid creating a block
    // region for null values.
    private final Block nullValueBlock;

    private final List<MapFlatValueWriter> valueWriters = new ArrayList<>();
    private final IntFunction<ColumnWriter> valueWriterFactory;
    private final Supplier<Map<Integer, ColumnStatistics>> emptyValueColumnStatisticsSupplier;
    private final IntList rowsInFinishedRowGroups = new IntArrayList();
    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private boolean closed;

    // Number of non-null rows in the current row group. Used for statistics and
    // to catch up value writers on missing keys
    private int nonNullRowGroupValueCount;
    private int nonNullStripeValueCount;

    // Contains the last row written by a certain value writer.
    // It is used to catch up on missing keys.
    private int[] valueWritersLastRow = new int[0];

    // If no value has been written, then valueWriters list will be empty, and we
    // won't be able to get required column statistics for all sub nodes.
    // This field contains cached stripe column statistics for the value node(s)
    // for this edge case.
    private Map<Integer, ColumnStatistics> emptyValueColumnStatistics;

    public MapFlatColumnWriter(
            int nodeIndex,
            int keyNodeIndex,
            int valueNodeIndex,
            Type keyType,
            Type valueType,
            Supplier<StatisticsBuilder> keyStatisticsBuilderSupplier,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            MetadataWriter metadataWriter,
            IntFunction<ColumnWriter> valueWriterFactory,
            Supplier<Map<Integer, ColumnStatistics>> emptyValueColumnStatisticsSupplier)
    {
        checkArgument(nodeIndex > 0, "nodeIndex is invalid: %s", nodeIndex);
        checkArgument(keyNodeIndex > 0, "keyNodeIndex is invalid: %s", keyNodeIndex);
        checkArgument(valueNodeIndex > 0, "valueNodeIndex is invalid: %s", valueNodeIndex);
        requireNonNull(keyStatisticsBuilderSupplier, "keyStatisticsBuilderSupplier is null");
        checkArgument(columnWriterOptions.getMaxFlattenedMapKeyCount() > 0, "maxFlattenedMapKeyCount must be positive: %s", columnWriterOptions.getMaxFlattenedMapKeyCount());

        this.nodeIndex = nodeIndex;
        this.keyNodeIndex = keyNodeIndex;
        this.valueNodeIndex = valueNodeIndex;
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.nullValueBlock = createNullValueBlock(requireNonNull(valueType, "valueType is null"));
        this.maxFlattenedMapKeyCount = columnWriterOptions.getMaxFlattenedMapKeyCount();
        this.mapStatsEnabled = columnWriterOptions.isMapStatisticsEnabled();

        this.columnWriterOptions = requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        this.dwrfEncryptor = requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        this.keyManager = getKeyManager(keyType, keyStatisticsBuilderSupplier);
        this.valueWriterFactory = requireNonNull(valueWriterFactory, "valueWriterFactory is null");
        this.emptyValueColumnStatisticsSupplier = requireNonNull(emptyValueColumnStatisticsSupplier, "emptyValueColumnStatisticsSupplier is null");

        this.compressed = columnWriterOptions.getCompressionKind() != NONE;
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        ImmutableList.Builder<ColumnWriter> nestedWriters = ImmutableList.builderWithExpectedSize(valueWriters.size() * 2);
        for (MapFlatValueWriter mapValueWriter : valueWriters) {
            ColumnWriter valueWriter = mapValueWriter.getValueWriter();
            nestedWriters.add(valueWriter);
            nestedWriters.addAll(valueWriter.getNestedColumnWriters());
        }
        return nestedWriters.build();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.<Integer, ColumnEncoding>builder()
                .put(nodeIndex, FLAT_MAP_COLUMN_ENCODING)
                .putAll(getValueColumnEncodings())
                .build();
    }

    private Map<Integer, ColumnEncoding> getValueColumnEncodings()
    {
        // Gather all sub encodings as nodeId -> Map<SequenceId, DwrfSequenceEncoding[dwrfKey, valueEncoding]>
        Map<Integer, ImmutableSortedMap.Builder<Integer, DwrfSequenceEncoding>> sequenceEncodingsByNode = new HashMap<>();
        for (MapFlatValueWriter valueWriter : valueWriters) {
            DwrfProto.KeyInfo dwrfKey = valueWriter.getDwrfKey();
            Integer sequence = valueWriter.getSequence();
            Map<Integer, ColumnEncoding> valueEncodings = valueWriter.getValueWriter().getColumnEncodings();

            valueEncodings.forEach((nodeIndex, valueEncoding) -> {
                ImmutableSortedMap.Builder<Integer, DwrfSequenceEncoding> sequenceEncodings =
                        sequenceEncodingsByNode.computeIfAbsent(nodeIndex, (ignore) -> ImmutableSortedMap.naturalOrder());
                sequenceEncodings.put(sequence, new DwrfSequenceEncoding(dwrfKey, valueEncoding));
            });
        }

        ImmutableMap.Builder<Integer, ColumnEncoding> columnEncoding = ImmutableMap.builder();

        // Put a parent ColumnEncoding on top of all collected sub encodings.
        // Kind of parent's ColumnEncodingKind doesn't matter, it will be ignored by the metadata writer.
        sequenceEncodingsByNode.forEach((nodeIndex, sequenceEncodings) -> {
            ColumnEncoding valueEncoding = new ColumnEncoding(DIRECT, 0, Optional.of(sequenceEncodings.build()));
            columnEncoding.put(nodeIndex, valueEncoding);
        });
        return columnEncoding.build();
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        valueWriters.forEach(MapFlatValueWriter::beginRowGroup);
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);

        // catch up on missing key before finishing the row group
        for (int i = 0; i < valueWriters.size(); i++) {
            if (valueWritersLastRow[i] < nonNullRowGroupValueCount) {
                valueWriters.get(i).writeNotInMap(nonNullRowGroupValueCount - valueWritersLastRow[i]);
            }
        }
        Arrays.fill(valueWritersLastRow, 0);

        Map<Integer, ColumnStatistics> columnStatistics = getColumnStatisticsFromValueWriters(ColumnWriter::finishRowGroup, nonNullRowGroupValueCount);
        ColumnStatistics mapStatistics = requireNonNull(columnStatistics.get(nodeIndex), "ColumnStatistics for the map node is missing");
        rowGroupColumnStatistics.add(mapStatistics);

        rowsInFinishedRowGroups.add(nonNullRowGroupValueCount);
        nonNullStripeValueCount += nonNullRowGroupValueCount;
        nonNullRowGroupValueCount = 0;

        return columnStatistics;
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);

        return ImmutableMap.<Integer, ColumnStatistics>builder()
                .put(keyNodeIndex, keyManager.getStripeColumnStatistics())
                .putAll(getColumnStatisticsFromValueWriters(ColumnWriter::getColumnStripeStatistics, nonNullStripeValueCount))
                .build();
    }

    /**
     * Create and cache ColumnStatistics for the value node(s) for cases when the there are no value
     * writers, but we still need to write valid column stripe statistics for all nested writers.
     */
    private Map<Integer, ColumnStatistics> getEmptyValueColumnStatistics()
    {
        if (emptyValueColumnStatistics == null) {
            // stats are the same for row group and stripe
            emptyValueColumnStatistics = emptyValueColumnStatisticsSupplier.get();
        }
        return emptyValueColumnStatistics;
    }

    /**
     * Returns merged stats from the value writers + map stats built on top of value node stats
     * Aggregates statistics of all value writers. A value column can be complex (it is a tree of nodes), we need to
     * aggregate every level of the tree, across all value writers.
     */
    private Map<Integer, ColumnStatistics> getColumnStatisticsFromValueWriters(Function<ColumnWriter, Map<Integer, ColumnStatistics>> getStats, int valueCount)
    {
        MapColumnStatisticsBuilder mapStatsBuilder = new MapColumnStatisticsBuilder(mapStatsEnabled);
        mapStatsBuilder.increaseValueCount(valueCount);

        // return some stats even if the map is empty
        if (valueWriters.isEmpty()) {
            return ImmutableMap.<Integer, ColumnStatistics>builder()
                    .put(nodeIndex, mapStatsBuilder.buildColumnStatistics())
                    .putAll(getEmptyValueColumnStatistics())
                    .build();
        }

        // collect statistics from all value writers, aggregate by the node
        ImmutableListMultimap.Builder<Integer, ColumnStatistics> allValueStats = ImmutableListMultimap.builder();
        for (MapFlatValueWriter valueWriter : valueWriters) {
            Map<Integer, ColumnStatistics> valueColumnStatistics = getStats.apply(valueWriter.getValueWriter());
            allValueStats.putAll(valueColumnStatistics.entrySet());

            // feed the value node statistics into the map statistics builder
            if (mapStatsEnabled) {
                ColumnStatistics valueNodeStats = valueColumnStatistics.get(valueNodeIndex);
                if (valueNodeStats != null) {
                    mapStatsBuilder.addMapStatistics(valueWriter.getDwrfKey(), valueNodeStats);
                }
            }
        }

        // merge multiple column statistics grouped by the node into a single statistics object for each node
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        allValueStats.build().asMap().forEach((node, nodeStats) -> {
            ColumnStatistics mergedNodeStats = mergeColumnStatistics((List<ColumnStatistics>) nodeStats);
            columnStatistics.put(node, mergedNodeStats);
        });

        // add map statistics
        columnStatistics.put(nodeIndex, mapStatsBuilder.buildColumnStatistics());

        return columnStatistics.build();
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "block is empty");

        ColumnarMap columnarMap = toColumnarMap(block);
        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();

        long childRawSize = 0;
        int nonNullValueCountBefore = nonNullRowGroupValueCount;

        // write key+value entries one by one
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            boolean present = !columnarMap.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                int entryCount = columnarMap.getEntryCount(position);
                int entryOffset = columnarMap.getOffset(position);
                for (int i = 0; i < entryCount; i++) {
                    childRawSize += writeMapKeyValue(keysBlock, valuesBlock, entryOffset + i);
                }
                nonNullRowGroupValueCount++;
            }
        }

        // TODO Implement size reporting
        int blockNonNullValueCount = nonNullRowGroupValueCount - nonNullValueCountBefore;
        return (columnarMap.getPositionCount() - blockNonNullValueCount) * NULL_SIZE + childRawSize;
    }

    private long writeMapKeyValue(Block keysBlock, Block valuesBlock, int position)
    {
        checkArgument(!keysBlock.isNull(position), "Flat map key cannot be null. Node %s", nodeIndex);
        MapFlatValueWriter valueWriter = keyManager.getOrCreateValueWriter(position, keysBlock);

        // catch up value writer on missing rows
        int valueWriterIdx = valueWriter.getSequence() - SEQUENCE_START_INDEX;
        if (valueWritersLastRow[valueWriterIdx] < nonNullRowGroupValueCount) {
            valueWriter.writeNotInMap(nonNullRowGroupValueCount - valueWritersLastRow[valueWriterIdx]);
        }
        valueWritersLastRow[valueWriterIdx] = nonNullRowGroupValueCount + 1;

        Block singleValueBlock;
        if (valuesBlock.isNull(position)) {
            singleValueBlock = nullValueBlock;
        }
        else {
            // TODO valueBlock.getRegion is really-really-really inefficient.
            //  Options:
            //  1. Create a new type of ColumnarMap optimized for flat maps;
            //  2. Extend ColumnWriter to write block regions;
            //  3. Write entries in batches using block.getPositions() to make it more efficient
            //     and keep memory consumption in check.
            singleValueBlock = valuesBlock.getRegion(position, 1);
        }
        return valueWriter.writeSingleEntryBlock(singleValueBlock);
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> streams = ImmutableList.builder();
        for (MapFlatValueWriter valueWriter : valueWriters) {
            streams.addAll(valueWriter.getDataStreams());
        }
        presentStream.getStreamDataOutput(nodeIndex, DEFAULT_SEQUENCE_ID).ifPresent(streams::add);
        return streams.build();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<? extends StreamCheckpoint>> prependCheckpoints)
            throws IOException
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> streams = ImmutableList.builder();

        // add map node row indexes
        List<RowGroupIndex> rowGroupIndexes = buildRowGroupIndexes(compressed, rowGroupColumnStatistics, prependCheckpoints, presentStream);
        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes);
        Stream stream = new Stream(nodeIndex, DEFAULT_SEQUENCE_ID, ROW_INDEX, slice.length(), false);
        streams.add(new StreamDataOutput(slice, stream));

        // add index streams for value nodes
        for (MapFlatValueWriter valueWriter : valueWriters) {
            streams.addAll(valueWriter.getIndexStreams());
        }
        return streams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        long bufferedBytes = presentStream.getBufferedBytes();
        for (MapFlatValueWriter valueWriter : valueWriters) {
            bufferedBytes += valueWriter.getBufferedBytes();
        }
        return bufferedBytes;
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = 0;
        for (MapFlatValueWriter valueWriter : valueWriters) {
            retainedBytes += valueWriter.getValueWriter().getRetainedBytes();
        }
        // TODO Implement me
        return INSTANCE_SIZE + retainedBytes;
    }

    @Override
    public void close()
    {
        closed = true;
        valueWriters.forEach(MapFlatValueWriter::close);
        presentStream.close();
    }

    @Override
    public void reset()
    {
        closed = false;
        presentStream.reset();
        keyManager.reset();
        valueWriters.clear();
        rowGroupColumnStatistics.clear();
        rowsInFinishedRowGroups.clear();
        nonNullRowGroupValueCount = 0;
        nonNullStripeValueCount = 0;
        Arrays.fill(valueWritersLastRow, 0);
    }

    private MapFlatValueWriter createNewValueWriter(DwrfProto.KeyInfo dwrfKey)
    {
        checkState(valueWriters.size() < maxFlattenedMapKeyCount - 1,
                "Map column writer for node %s reached max allowed number of keys %s", nodeIndex, maxFlattenedMapKeyCount);

        int valueWriterIdx = valueWriters.size();
        int sequence = valueWriterIdx + SEQUENCE_START_INDEX;
        ColumnWriter columnWriter = valueWriterFactory.apply(sequence);
        MapFlatValueWriter valueWriter = new MapFlatValueWriter(valueNodeIndex, sequence, dwrfKey, columnWriter, columnWriterOptions, dwrfEncryptor);
        valueWriters.add(valueWriter);
        growCapacity();

        // catch up on missing rows
        valueWriter.writeNotInMap(rowsInFinishedRowGroups, nonNullRowGroupValueCount);
        valueWritersLastRow[valueWriterIdx] = nonNullRowGroupValueCount + 1;

        return valueWriter;
    }

    // Make sure the size of valueWritersLastRow is same or larger than the number of value writers
    private void growCapacity()
    {
        if (valueWritersLastRow.length < valueWriters.size()) {
            valueWritersLastRow = ensureCapacity(valueWritersLastRow, Math.max(MIN_CAPACITY, valueWriters.size()), MEDIUM, PRESERVE);
        }
    }

    private static Block createNullValueBlock(Type valueType)
    {
        if (valueType instanceof FixedWidthType) {
            FixedWidthType fixedWidthType = (FixedWidthType) valueType;
            return fixedWidthType.createFixedSizeBlockBuilder(1).appendNull().build();
        }
        return valueType.createBlockBuilder(null, 1).appendNull().build();
    }

    private KeyManager getKeyManager(Type type, Supplier<StatisticsBuilder> statisticsBuilderSupplier)
    {
        if (type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT) {
            return new NumericKeyManager(statisticsBuilderSupplier);
        }
        else if (type instanceof VarcharType || type == VARBINARY) {
            return new SliceKeyManager(statisticsBuilderSupplier);
        }
        throw new IllegalArgumentException("Unsupported flat map key type: " + type);
    }

    private abstract static class KeyManager<T extends Map<?, MapFlatValueWriter>>
    {
        protected StatisticsBuilder rowGroupStatsBuilder;
        protected final T keyToWriter;
        private final Supplier<StatisticsBuilder> statisticsBuilderSupplier;

        public KeyManager(T keyToWriter, Supplier<StatisticsBuilder> statisticsBuilderSupplier)
        {
            this.keyToWriter = requireNonNull(keyToWriter, "keyToWriter is null");
            this.statisticsBuilderSupplier = requireNonNull(statisticsBuilderSupplier, "statisticsBuilderSupplier is null");
            this.rowGroupStatsBuilder = statisticsBuilderSupplier.get();
        }

        public abstract MapFlatValueWriter getOrCreateValueWriter(int position, Block keyBlock);

        public ColumnStatistics getStripeColumnStatistics()
        {
            return rowGroupStatsBuilder.buildColumnStatistics();
        }

        public void reset()
        {
            keyToWriter.clear();
            rowGroupStatsBuilder = statisticsBuilderSupplier.get();
        }
    }

    // Key manager for byte, short, int, long keys.
    private class NumericKeyManager
            extends KeyManager<Long2ObjectOpenHashMap<MapFlatValueWriter>>
    {
        public NumericKeyManager(Supplier<StatisticsBuilder> statisticsBuilderSupplier)
        {
            super(new Long2ObjectOpenHashMap<>(), statisticsBuilderSupplier);
        }

        @Override
        public MapFlatValueWriter getOrCreateValueWriter(int position, Block keyBlock)
        {
            rowGroupStatsBuilder.addValue(keyType, keyBlock, position);

            long key = keyType.getLong(keyBlock, position);
            MapFlatValueWriter valueWriter = keyToWriter.get(key);
            if (valueWriter == null) {
                valueWriter = createNewValueWriter(createDwrfKey(key));
                keyToWriter.put(key, valueWriter);
            }
            return valueWriter;
        }

        private DwrfProto.KeyInfo createDwrfKey(long key)
        {
            return DwrfProto.KeyInfo.newBuilder().setIntKey(key).build();
        }
    }

    // Key manager for string and byte[] keys
    private class SliceKeyManager
            extends KeyManager<Map<Slice, MapFlatValueWriter>>
    {
        public SliceKeyManager(Supplier<StatisticsBuilder> statisticsBuilderSupplier)
        {
            super(new HashMap<>(), statisticsBuilderSupplier);
        }

        @Override
        public MapFlatValueWriter getOrCreateValueWriter(int position, Block keyBlock)
        {
            rowGroupStatsBuilder.addValue(keyType, keyBlock, position);

            Slice key = keyType.getSlice(keyBlock, position);
            MapFlatValueWriter valueWriter = keyToWriter.get(key);
            if (valueWriter == null) {
                if (!key.isCompact()) {
                    key = Slices.copyOf(key);
                }
                valueWriter = createNewValueWriter(createDwrfKey(key));
                keyToWriter.put(key, valueWriter);
            }
            return valueWriter;
        }

        private DwrfProto.KeyInfo createDwrfKey(Slice key)
        {
            int sliceLength = key.length();
            DwrfProto.KeyInfo dwrfKey;
            if (sliceLength == 0) {
                dwrfKey = EMPTY_SLICE_KEY;
            }
            else {
                ByteString byteString = ByteString.copyFrom(key.getBytes(), 0, sliceLength);
                dwrfKey = DwrfProto.KeyInfo.newBuilder().setBytesKey(byteString).build();
            }
            return dwrfKey;
        }
    }
}
