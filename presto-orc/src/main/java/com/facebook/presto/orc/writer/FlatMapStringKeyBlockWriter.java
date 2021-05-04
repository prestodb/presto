package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

public class FlatMapStringKeyBlockWriter
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
    private Map<String, FlatMapKeyNodeWriter> keyToKeyNode = new HashMap<>();

    public FlatMapStringKeyBlockWriter(
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

    public String getStringValue(Block keysBlock, int position)
    {
        String keyAsStr = null;
        SqlFunctionProperties PROPERTIES = SqlFunctionProperties
                .builder()
                .setTimeZoneKey(UTC_KEY)
                .setLegacyTimestamp(true)
                .setSessionStartTime(0)
                .setSessionLocale(ENGLISH)
                .setSessionUser("user")
                .build();

        switch (orcKeyType.getOrcTypeKind()) {
            case CHAR:
                char charKey = (char) this.keyType.getObjectValue(PROPERTIES, keysBlock, position);
                keyAsStr = String.valueOf(charKey);
                break;
                // fall through
            case VARCHAR:
            case STRING:
                keyAsStr = (String) this.keyType.getObjectValue(PROPERTIES, keysBlock, position);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + orcKeyType.getOrcTypeKind());
        }
        return keyAsStr;
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
                String keyAsStr = getStringValue(keysBlock, offset + entry);
                Block valueBlock = valuesBlock.getRegion(offset + entry, 1);

                boolean keyExists = keyToKeyNode.containsKey(keyAsStr);
                if (!keyExists) {
                    FlatMapKeyNodeWriter newKeyNode = new FlatMapKeyNodeWriter(this.valueColumnId, this.valueSequenceCounter, this.columnWriterOptions, this.dwrfEncryptor, this.metadataWriter, valueWriterFactory.getColumnWriter(this.valueSequenceCounter));
                    newKeyNode.beginRowGroup();
                    newKeyNode.addNulls(position);
                    keyToKeyNode.putIfAbsent(keyAsStr, newKeyNode);
                    this.valueSequenceCounter++;
                    keyNodes.add(keyToKeyNode.get(keyAsStr));
                }
                FlatMapKeyNodeWriter node = keyToKeyNode.get(keyAsStr);
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
        return ImmutableList.of();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of();
    }

    @Override
    public void beginRowGroup()
    {
        BiConsumer<String, FlatMapKeyNodeWriter> biConsumer = (key,value) ->
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
        return ImmutableMap.of();
    }

    @Override
    public void close()
    {
        closed = true;
        BiConsumer<String, FlatMapKeyNodeWriter> biConsumer = (key,value) -> value.close();
        keyToKeyNode.forEach(biConsumer);
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of();
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
        return ImmutableList.of();
    }

    /**
     * Get the data streams to be written.
     */
    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        return ImmutableList.of();
    }

    /**
     * This method returns the size of the flushed data plus any unflushed data.
     * If the output is compressed, flush data size is the size after compression.
     */
    @Override
    public long getBufferedBytes()
    {
        AtomicLong valueWritersBufferedBytes = new AtomicLong();
        BiConsumer<String, FlatMapKeyNodeWriter> biConsumer = (key,value) -> valueWritersBufferedBytes.getAndAdd(value.getBufferedBytes());
        keyToKeyNode.forEach(biConsumer);
        return valueWritersBufferedBytes.longValue();
    }

    @Override
    public long getRetainedBytes()
    {
        AtomicLong valueWritersRetainedBytes = new AtomicLong();
        BiConsumer<String, FlatMapKeyNodeWriter> biConsumer = (key,value) -> valueWritersRetainedBytes.getAndAdd(value.getRetainedBytes());
        keyToKeyNode.forEach(biConsumer);
        return valueWritersRetainedBytes.longValue();
    }

    @Override
    public void reset()
    {
        closed = false;
        BiConsumer<String, FlatMapKeyNodeWriter> biConsumer = (key,value) -> value.reset();
        keyToKeyNode.forEach(biConsumer);
    }
}
