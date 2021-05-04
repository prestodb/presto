package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.getIntKeyInfo;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

public class FlatMapNumericKeyBlockWriter
        implements FlatMapBlockWriter
{
    private final int valueColumnId;
    private final Type keyType;
    private final Type valueType;
    private final OrcType orcKeyType;
    ColumnWriterOptions columnWriterOptions;
    private final Optional<DwrfDataEncryptor> dwrfEncryptor;
    private final MetadataWriter metadataWriter;
    private final ColumnWriterFactory valueWriterFactory;
    private int valueSequenceCounter;
    private List<FlatMapKeyNodeWriter> keyNodes;
    private boolean closed;
    private Long2ObjectMap<FlatMapKeyNodeWriter> keyToKeyNode = new Long2ObjectOpenHashMap<>();

    public FlatMapNumericKeyBlockWriter(
            int valueColumnId,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            MetadataWriter metadataWriter,
            Type type,
            OrcType orcKeyType,
            ColumnWriterFactory valueWriterFactory,
            List<FlatMapKeyNodeWriter> keyNodes)
    {
        this.valueColumnId = valueColumnId;
        MapType mapType = (MapType) type;
        this.keyType = mapType.getTypeParameters().get(0);
        this.valueType = mapType.getTypeParameters().get(1);
        this.orcKeyType = orcKeyType;

        this.columnWriterOptions = columnWriterOptions;
        this.dwrfEncryptor = dwrfEncryptor;
        this.metadataWriter = metadataWriter;
        this.valueWriterFactory = valueWriterFactory;
        this.keyNodes = keyNodes;

        this.valueSequenceCounter = 1;
    }

    public Long getLongValue(Block keysBlock, int position)
    {
        Long keyAsLong;
        SqlFunctionProperties PROPERTIES = SqlFunctionProperties
                .builder()
                .setTimeZoneKey(UTC_KEY)
                .setLegacyTimestamp(true)
                .setSessionStartTime(0)
                .setSessionLocale(ENGLISH)
                .setSessionUser("user")
                .build();

        switch (orcKeyType.getOrcTypeKind()) {
            case SHORT:
                short shortKey = (short) this.keyType.getObjectValue(PROPERTIES, keysBlock, position);
                keyAsLong = (long) shortKey;
                break;
            case INT:
                int intKey = (int) this.keyType.getObjectValue(PROPERTIES, keysBlock, position);
                keyAsLong = (long) intKey;
                break;
            case LONG:
                long longKey = (long) this.keyType.getObjectValue(PROPERTIES, keysBlock, position);
                keyAsLong = longKey;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + orcKeyType.getOrcTypeKind());
        }
        return keyAsLong;
    }

    @Override
    public void writeColumnarMap(ColumnarMap columnarMap)
    {
        // write keys and value
        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();

        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            boolean present = !columnarMap.isNull(position);
            if (!present)
                continue;

            int offset = columnarMap.getOffset(position);
            int entryCount = columnarMap.getEntryCount(position);
            /* This hits array records all the sequence ids 1...n that have a entry for this row */
            final boolean[] hits = new boolean[keyNodes.size() + 1];
            for (int entry = 0; entry < entryCount; entry++) {
                /* Get the key as a Long */
                Long keyAsLong = getLongValue(keysBlock, offset + entry);
                /* Get the value for this key as a Block */
                Block valueBlock = valuesBlock.getRegion(offset + entry, 1);

                boolean keyExists = keyToKeyNode.containsKey(keyAsLong.longValue());
                if (!keyExists) {
                    FlatMapKeyNodeWriter newKeyNode = new FlatMapKeyNodeWriter(this.valueColumnId, this.valueSequenceCounter, this.columnWriterOptions, this.dwrfEncryptor, this.metadataWriter, valueWriterFactory.getColumnWriter(this.valueSequenceCounter));
                    newKeyNode.beginRowGroup();
                    newKeyNode.addNulls(position);
                    keyToKeyNode.putIfAbsent(keyAsLong.longValue(), newKeyNode);
                    this.valueSequenceCounter++;
                    keyNodes.add(keyToKeyNode.get(keyAsLong.longValue()));
                }
                FlatMapKeyNodeWriter node = keyToKeyNode.get(keyAsLong.longValue());
                node.writeBlock(valueBlock);

                if(keyExists)
                    hits[node.getSequence()] = true;
            }

            /* for all the keys that do not have entries for this row append null */
            for (int i = 1; i < hits.length; i++) {
                if(!hits[i]) {
                    keyNodes.get(i).addNulls(1);
                }
            }
        }
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        ImmutableList.Builder<ColumnWriter> columnWriters = ImmutableList.builder();
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> columnWriters.addAll(value.getNestedColumnWriters());
        keyToKeyNode.forEach(biConsumer);
        return columnWriters.build();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();

        /* Build the additionalEncodings for each of the sequences */
        SortedMap<Integer, DwrfSequenceEncoding> additionalEncodings = new TreeMap<Integer, DwrfSequenceEncoding>();
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> {
            Map<Integer, ColumnEncoding> valueEncodings = value.getColumnEncodings();
            for(Map.Entry<Integer, ColumnEncoding> entry : valueEncodings.entrySet()) {
                additionalEncodings.put(entry.getKey(), new DwrfSequenceEncoding(getIntKeyInfo(key), entry.getValue()));
            }
        };
        keyToKeyNode.forEach(biConsumer);
        ColumnEncoding valueColumnEncoding = new ColumnEncoding(DIRECT, 0, Optional.of(additionalEncodings));
        encodings.put(this.valueColumnId, valueColumnEncoding);
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) ->
                value.beginRowGroup();
        keyToKeyNode.forEach(biConsumer);
    }

    @Override
    public void writeBlock(Block block)
    {

    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        ImmutableList.Builder<ColumnStatistics> columnStatistics = ImmutableList.builder();
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> {
            columnStatistics.addAll(
                    value.finishRowGroup().entrySet().stream().map(entry -> entry.getValue()).collect(toList())
            );
        };
        keyToKeyNode.forEach(biConsumer);
        return ImmutableMap.of(valueColumnId, ColumnStatistics.mergeColumnStatistics(columnStatistics.build()));
    }

    @Override
    public void close()
    {
        closed = true;
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> value.close();
        keyToKeyNode.forEach(biConsumer);
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        ImmutableList.Builder<ColumnStatistics> columnStatistics = ImmutableList.builder();
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> {
            columnStatistics.addAll(
                    value.getColumnStripeStatistics().entrySet().stream().map(entry -> entry.getValue()).collect(toList())
            );
        };
        keyToKeyNode.forEach(biConsumer);
        return ImmutableMap.of(valueColumnId, ColumnStatistics.mergeColumnStatistics(columnStatistics.build()));
    }

    /**
     * Write index streams to the output and return the streams in the
     * order in which they were written.  The ordering is critical because
     * the stream only contain a length with no offset.
     */
    @Override
    public List<StreamDataOutput> getIndexStreams()
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> indexStreams = ImmutableList.builder();
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> {
            try {
                indexStreams.addAll(value.getIndexStreams());
            }
            catch (IOException e) {
                try {
                    throw e;
                }
                catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        };
        keyToKeyNode.forEach(biConsumer);
        return indexStreams.build();
    }

    /**
     * Get the data streams to be written.
     */
    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();

        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> outputDataStreams.addAll(value.getDataStreams());
        keyToKeyNode.forEach(biConsumer);
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
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> valueWritersBufferedBytes.getAndAdd(value.getBufferedBytes());
        keyToKeyNode.forEach(biConsumer);
        return valueWritersBufferedBytes.longValue();
    }

    @Override
    public long getRetainedBytes()
    {
        AtomicLong valueWritersRetainedBytes = new AtomicLong();
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> valueWritersRetainedBytes.getAndAdd(value.getRetainedBytes());
        keyToKeyNode.forEach(biConsumer);
        return valueWritersRetainedBytes.longValue();
    }

    @Override
    public void reset()
    {
        closed = false;
        BiConsumer<Long, FlatMapKeyNodeWriter> biConsumer = (key,value) -> value.reset();
        keyToKeyNode.forEach(biConsumer);
    }
}
