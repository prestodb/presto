
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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.getBytesKeyInfo;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.getIntKeyInfo;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;

public class FlatMapColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FlatMapColumnWriter.class).instanceSize();

    private final int column;
    private final int keyColumnId;
    private final int valueColumnId;
    private final MapType type;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final CompressedMetadataWriter metadataWriter;
    private final PresentOutputStream presentStream;

    /* Fields for capturing column statistics */
    private int nonNullValueCount;
    private StatisticsBuilder statisticsBuilder;
    private final Supplier<StatisticsBuilder> statisticsBuilderSupplier;
    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private final List<ColumnStatistics> keyRowGroupColumnStatistics = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;
    private final int stringStatisticsLimitInBytes;

    /* valueWriterFactory to generate streams for individual keys */
    private final FlatMapValueWriterFactory valueWriterFactory;
    private final FlatMapKeyToValueMap keyToValueNodes;
    private int valueSequenceCounter;
    private int rowsTillNow;
    private List<Integer> rowGroupRowCount;

    private boolean closed;

    private StringStatisticsBuilder newStringStatisticsBuilder()
    {
        return new StringStatisticsBuilder(stringStatisticsLimitInBytes);
    }

    public boolean isValidNumericType(OrcType.OrcTypeKind type)
    {
        switch (type) {
            case SHORT:
            case INT:
            case LONG:
                return true;
            case VARCHAR:
            case STRING:
                return false;
            default:
                throw new IllegalArgumentException("Unsupported type for FlatMap Keys : " + type);
        }
    }

    public FlatMapColumnWriter(
            int column,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter,
            Type type,
            List<OrcType> orcTypes,
            FlatMapValueWriterFactory valueWriterfactory)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(orcEncoding == DWRF, "Flat Maps are only supported for DWRF Encoding");

        this.column = column;
        this.keyColumnId = orcTypes.get(column).getFieldTypeIndex(0);
        this.valueColumnId = orcTypes.get(column).getFieldTypeIndex(1);
        this.type = (MapType) type;
        this.compressed = columnWriterOptions.getCompressionKind() != NONE;
        this.columnEncoding = new ColumnEncoding(DWRF_MAP_FLAT, 0);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);

        this.valueSequenceCounter = 1;
        this.valueWriterFactory = valueWriterfactory;
        this.rowGroupRowCount = new ArrayList<>();
        OrcTypeKind keyOrcTypeKind = orcTypes.get(keyColumnId).getOrcTypeKind();
        if (isValidNumericType(keyOrcTypeKind)) {
            this.keyToValueNodes = new FlatMapKeyToNumericValueMap(keyOrcTypeKind);
        }
        else {
            this.keyToValueNodes = new FlatMapKeyToStringValueMap(keyOrcTypeKind);
        }

        /* Setup column statistics builder */
        this.stringStatisticsLimitInBytes = toIntExact(columnWriterOptions.getStringStatisticsLimit().toBytes());
        if (keyToValueNodes.isNumericKey()) {
            this.statisticsBuilderSupplier = IntegerStatisticsBuilder::new;
        }
        else {
            this.statisticsBuilderSupplier = this::newStringStatisticsBuilder;
        }
        this.statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();

        encodings.put(column, columnEncoding);

        /* Build the additionalEncodings for each of the sequences */
        SortedMap<Integer, DwrfSequenceEncoding> additionalEncodings = new TreeMap<>();
        if (keyToValueNodes.isNumericKey()) {
            BiConsumer<Long, FlatMapValueColumnWriter> biConsumer = (key, value) -> {
                Map<Integer, ColumnEncoding> valueEncodings = value.getColumnEncodings();
                for (Map.Entry<Integer, ColumnEncoding> entry : valueEncodings.entrySet()) {
                    additionalEncodings.put(entry.getKey(), new DwrfSequenceEncoding(getIntKeyInfo(key), entry.getValue()));
                }
            };
            keyToValueNodes.forEach(biConsumer);
        }
        else {
            BiConsumer<String, FlatMapValueColumnWriter> biConsumer = (key, value) -> {
                Map<Integer, ColumnEncoding> valueEncodings = value.getColumnEncodings();
                for (Map.Entry<Integer, ColumnEncoding> entry : valueEncodings.entrySet()) {
                    additionalEncodings.put(entry.getKey(), new DwrfSequenceEncoding(getBytesKeyInfo(ByteString.copyFrom(key.getBytes())), entry.getValue()));
                }
            };
            keyToValueNodes.forEach(biConsumer);
        }
        ColumnEncoding valueColumnEncoding = new ColumnEncoding(DIRECT, 0, Optional.of(additionalEncodings));
        encodings.put(this.valueColumnId, valueColumnEncoding);
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) ->
                value.beginRowGroup();
        keyToValueNodes.forEach(biConsumer);
    }

    public void writeColumnarMap(ColumnarMap columnarMap)
    {
        // write keys and value
        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();

        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            boolean present = !columnarMap.isNull(position);
            if (!present) {
                continue;
            }

            int offset = columnarMap.getOffset(position);
            int entryCount = columnarMap.getEntryCount(position);
            for (int entry = 0; entry < entryCount; entry++) {
                boolean keyExists = keyToValueNodes.containsKey(keysBlock, offset + entry, type.getKeyType());
                FlatMapValueColumnWriter keyNode;
                if (!keyExists) {
                    keyNode = valueWriterFactory.getFlatMapValueColumnWriter(this.valueSequenceCounter++);
                    keyNode.finishRowGroups(rowGroupRowCount);
                    keyNode.beginRowGroup();
                    keyToValueNodes.putIfAbsent(keysBlock, entry + offset, type.getKeyType(), keyNode);
                }
                else {
                    keyNode = keyToValueNodes.get(keysBlock, entry + offset, type.getKeyType());
                }

                /* Get the value for this key as a Block */
                Block valueBlock = valuesBlock.getRegion(offset + entry, 1);
                keyNode.writeBlock(valueBlock, rowsTillNow);
            }
            rowsTillNow++;
        }
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarMap columnarMap = toColumnarMap(block);
        // write nulls and lengths
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            boolean present = !columnarMap.isNull(position);
            presentStream.writeBoolean(present);
            if (present) {
                nonNullValueCount++;
            }
        }
        writeColumnarMap(columnarMap);
        this.statisticsBuilder.addBlock(type.getKeyType(), columnarMap.getKeysBlock());
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);

        /* Map Type column statistics is just the nonNullValueCount */
        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, 0, null, null, null, null, null, null, null, null);
        rowGroupColumnStatistics.add(statistics);
        columnStatisticsRetainedSizeInBytes += statistics.getRetainedSizeInBytes();
        nonNullValueCount = 0;

        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        /* Add statistics for map type */
        columnStatistics.put(column, statistics);

        /* Add statistics for the key Type */
        ColumnStatistics keyStatistics = statisticsBuilder.buildColumnStatistics();
        keyRowGroupColumnStatistics.add(keyStatistics);
        columnStatisticsRetainedSizeInBytes += keyStatistics.getRetainedSizeInBytes();
        columnStatistics.put(keyColumnId, keyStatistics);
        statisticsBuilder = statisticsBuilderSupplier.get();

        /* Add statistics for the value Type */

        ImmutableList.Builder<ColumnStatistics> valueColumnStatistics = ImmutableList.builder();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> {
            valueColumnStatistics.addAll(
                    value.finishRowGroup(rowsTillNow)
                            .entrySet()
                            .stream()
                            .map(entry -> entry.getValue())
                            .collect(toList()));
        };
        keyToValueNodes.forEach(biConsumer);
        rowGroupRowCount.add(rowsTillNow);
        rowsTillNow = 0;

        columnStatistics.put(
                valueColumnId,
                ColumnStatistics.mergeColumnStatistics(valueColumnStatistics.build()));
        return columnStatistics.build();
    }

    @Override
    public void close()
    {
        closed = true;
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> value.close();
        keyToValueNodes.forEach(biConsumer);
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        /* Add stats for map type */
        columnStatistics.put(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
        /* add stats for key type */
        columnStatistics.put(keyColumnId, ColumnStatistics.mergeColumnStatistics(keyRowGroupColumnStatistics));
        /* add stats for value type */
        ImmutableList.Builder<ColumnStatistics> valueColumnStatistics = ImmutableList.builder();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> {
            valueColumnStatistics.addAll(
                    value.getColumnStripeStatistics()
                            .entrySet()
                            .stream()
                            .map(entry -> entry.getValue())
                            .collect(toList()));
        };
        keyToValueNodes.forEach(biConsumer);
        columnStatistics.putAll(ImmutableMap.of(valueColumnId,
                ColumnStatistics.mergeColumnStatistics(valueColumnStatistics.build())));
        return columnStatistics.build();
    }

    /**
     * Write index streams to the output and return the streams in the
     * order in which they were written.  The ordering is critical because
     * the stream only contain a length with no offset.
     */
    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<BooleanStreamCheckpoint>> inMapStreamCheckpoints)
            throws IOException
    {
        checkState(closed);
        checkArgument(!inMapStreamCheckpoints.isPresent(), "Flat Map ColumnWriter called with inMapStreamCheckpoints");
        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        /* Create the ROW INDEX Stream for the MAP type */
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createFlatMapColumnPositionList(compressed, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }
        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, DEFAULT_SEQUENCE_ID, Stream.StreamKind.ROW_INDEX, slice.length(), false, Optional.empty());
        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        indexStreams.add(new StreamDataOutput(slice, stream));

        /* There are no ROW_INDEX streams for key type in Flat Maps
         * Instead each sequence in value type has a ROW_INDEX stream  */
        ImmutableList.Builder<StreamDataOutput> valueIndexStreams = ImmutableList.builder();
        if (keyToValueNodes.isNumericKey()) {
            for (Long2ObjectMap.Entry<FlatMapValueColumnWriter> entry : keyToValueNodes.getLongKeyEntrySet()) {
                indexStreams.addAll(entry.getValue().getIndexStreams(Optional.empty()));
            }
        }
        else {
            for (Map.Entry<String, FlatMapValueColumnWriter> entry : keyToValueNodes.getStringKeyEntrySet()) {
                indexStreams.addAll(entry.getValue().getIndexStreams(Optional.empty()));
            }
        }
        indexStreams.addAll(valueIndexStreams.build());
        return indexStreams.build();
    }

    private static List<Integer> createFlatMapColumnPositionList(
            boolean compressed,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        return positionList.build();
    }

    /**
     * Get the data streams to be written.
     */
    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> outputDataStreams.addAll(value.getDataStreams());
        keyToValueNodes.forEach(biConsumer);
        presentStream.getStreamDataOutput(column, DEFAULT_SEQUENCE_ID).ifPresent(outputDataStreams::add);
        return outputDataStreams.build();
    }

    /**
     * This method returns the size of the flushed data plus any unflushed data.
     * If the output is compressed, flush data size is the size after compression.
     */
    @Override
    public long getBufferedBytes()
    {
        AtomicLong valueWritersBufferedBytes = new AtomicLong();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> valueWritersBufferedBytes.getAndAdd(value.getBufferedBytes());
        keyToValueNodes.forEach(biConsumer);
        return presentStream.getBufferedBytes() + valueWritersBufferedBytes.longValue();
    }

    @Override
    public long getRetainedBytes()
    {
        AtomicLong valueWritersRetainedBytes = new AtomicLong();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> valueWritersRetainedBytes.getAndAdd(value.getRetainedBytes());
        keyToValueNodes.forEach(biConsumer);
        return INSTANCE_SIZE +
                presentStream.getRetainedBytes() +
                valueWritersRetainedBytes.longValue() +
                columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        valueSequenceCounter = 1;
        presentStream.reset();
        BiConsumer<?, FlatMapValueColumnWriter> biConsumer = (key, value) -> value.reset();
        keyToValueNodes.forEach(biConsumer);
        rowGroupRowCount.clear();
        rowsTillNow = 0;
        rowGroupColumnStatistics.clear();
        keyRowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        nonNullValueCount = 0;
        statisticsBuilder = statisticsBuilderSupplier.get();
    }
}
