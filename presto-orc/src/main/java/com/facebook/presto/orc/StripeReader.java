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
package com.facebook.presto.orc;

import com.facebook.presto.orc.checkpoint.InvalidCheckpointException;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.HiveBloomFilter;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.orc.stream.ValueStream;
import com.facebook.presto.orc.stream.ValueStreamSource;
import com.facebook.presto.orc.stream.ValueStreams;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.orc.checkpoint.Checkpoints.getDictionaryStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.Checkpoints.getStreamCheckpoints;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.BLOOM_FILTER;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_COUNT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.facebook.presto.orc.stream.CheckpointStreamSource.createCheckpointStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class StripeReader
{
    private final OrcDataSource orcDataSource;
    private final CompressionKind compressionKind;
    private final List<OrcType> types;
    private final int bufferSize;
    private final HiveWriterVersion hiveWriterVersion;
    private final Set<Integer> includedOrcColumns;
    private final int rowsInRowGroup;
    private final OrcPredicate predicate;
    private final MetadataReader metadataReader;

    public StripeReader(OrcDataSource orcDataSource,
            CompressionKind compressionKind,
            List<OrcType> types,
            int bufferSize,
            Set<Integer> includedColumns,
            int rowsInRowGroup,
            OrcPredicate predicate,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader)
    {
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.bufferSize = bufferSize;
        this.includedOrcColumns = getIncludedOrcColumns(types, requireNonNull(includedColumns, "includedColumns is null"));
        this.rowsInRowGroup = rowsInRowGroup;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.hiveWriterVersion = requireNonNull(hiveWriterVersion, "hiveWriterVersion is null");
        this.metadataReader = requireNonNull(metadataReader, "metadataReader is null");
    }

    public Stripe readStripe(StripeInformation stripe, AggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        // read the stripe footer
        StripeFooter stripeFooter = readStripeFooter(stripe, systemMemoryUsage);
        List<ColumnEncoding> columnEncodings = stripeFooter.getColumnEncodings();

        // get streams for selected columns
        Map<StreamId, Stream> streams = new HashMap<>();
        boolean hasRowGroupDictionary = false;
        for (Stream stream : stripeFooter.getStreams()) {
            if (includedOrcColumns.contains(stream.getColumn())) {
                streams.put(new StreamId(stream), stream);

                ColumnEncodingKind columnEncoding = columnEncodings.get(stream.getColumn()).getColumnEncodingKind();
                if (columnEncoding == DICTIONARY && stream.getStreamKind() == StreamKind.IN_DICTIONARY) {
                    hasRowGroupDictionary = true;
                }
            }
        }

        // handle stripes with more than one row group or a dictionary
        if ((stripe.getNumberOfRows() > rowsInRowGroup) || hasRowGroupDictionary) {
            // determine ranges of the stripe to read
            Map<StreamId, DiskRange> diskRanges = getDiskRanges(stripeFooter.getStreams());
            diskRanges = Maps.filterKeys(diskRanges, Predicates.in(streams.keySet()));

            // read the file regions
            Map<StreamId, OrcInputStream> streamsData = readDiskRanges(stripe.getOffset(), diskRanges, systemMemoryUsage);

            // read the bloom filter for each column
            Map<Integer, List<HiveBloomFilter>> bloomFilterIndexes = readBloomFilterIndexes(streams, streamsData);

            // read the row index for each column
            Map<Integer, List<RowGroupIndex>> columnIndexes = readColumnIndexes(streams, streamsData, bloomFilterIndexes);

            // select the row groups matching the tuple domain
            Set<Integer> selectedRowGroups = selectRowGroups(stripe, columnIndexes);

            // if all row groups are skipped, return null
            if (selectedRowGroups.isEmpty()) {
                // set accounted memory usage to zero
                systemMemoryUsage.close();
                return null;
            }

            // value streams
            Map<StreamId, ValueStream<?>> valueStreams = createValueStreams(streams, streamsData, columnEncodings);

            // build the dictionary streams
            StreamSources dictionaryStreamSources = createDictionaryStreamSources(streams, valueStreams, columnEncodings);

            // build the row groups
            try {
                List<RowGroup> rowGroups = createRowGroups(
                        stripe.getNumberOfRows(),
                        streams,
                        valueStreams,
                        columnIndexes,
                        selectedRowGroups,
                        columnEncodings);

                return new Stripe(stripe.getNumberOfRows(), columnEncodings, rowGroups, dictionaryStreamSources);
            }
            catch (InvalidCheckpointException e) {
                // The ORC file contains a corrupt checkpoint stream
                // If the file does not have a row group dictionary, treat the stripe as a single row group. Otherwise,
                // we must fail because the length of the row group dictionary is contained in the checkpoint stream.
                if (hasRowGroupDictionary) {
                    throw new OrcCorruptionException(e, "ORC file %s has corrupt checkpoints", orcDataSource);
                }
            }
        }

        // stripe only has one row group and no dictionary
        ImmutableMap.Builder<StreamId, DiskRange> diskRangesBuilder = ImmutableMap.builder();
        for (Entry<StreamId, DiskRange> entry : getDiskRanges(stripeFooter.getStreams()).entrySet()) {
            StreamId streamId = entry.getKey();
            if (streamId.getStreamKind() != ROW_INDEX && streams.keySet().contains(streamId)) {
                diskRangesBuilder.put(entry);
            }
        }
        ImmutableMap<StreamId, DiskRange> diskRanges = diskRangesBuilder.build();

        // read the file regions
        Map<StreamId, OrcInputStream> streamsData = readDiskRanges(stripe.getOffset(), diskRanges, systemMemoryUsage);

        // value streams
        Map<StreamId, ValueStream<?>> valueStreams = createValueStreams(streams, streamsData, columnEncodings);

        // build the dictionary streams
        StreamSources dictionaryStreamSources = createDictionaryStreamSources(streams, valueStreams, columnEncodings);

        // build the row group
        ImmutableMap.Builder<StreamId, StreamSource<?>> builder = ImmutableMap.builder();
        for (Entry<StreamId, ValueStream<?>> entry : valueStreams.entrySet()) {
            builder.put(entry.getKey(), new ValueStreamSource<>(entry.getValue()));
        }
        RowGroup rowGroup = new RowGroup(0, 0, stripe.getNumberOfRows(), new StreamSources(builder.build()));

        return new Stripe(stripe.getNumberOfRows(), columnEncodings, ImmutableList.of(rowGroup), dictionaryStreamSources);
    }

    public Map<StreamId, OrcInputStream> readDiskRanges(long stripeOffset, Map<StreamId, DiskRange> diskRanges, AbstractAggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        //
        // Note: this code does not use the Java 8 stream APIs to avoid any extra object allocation
        //

        // transform ranges to have an absolute offset in file
        ImmutableMap.Builder<StreamId, DiskRange> diskRangesBuilder = ImmutableMap.builder();
        for (Entry<StreamId, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            diskRangesBuilder.put(entry.getKey(), new DiskRange(stripeOffset + diskRange.getOffset(), diskRange.getLength()));
        }
        diskRanges = diskRangesBuilder.build();

        // read ranges
        Map<StreamId, FixedLengthSliceInput> streamsData = orcDataSource.readFully(diskRanges);

        // transform streams to OrcInputStream
        String sourceName = orcDataSource.toString();
        ImmutableMap.Builder<StreamId, OrcInputStream> streamsBuilder = ImmutableMap.builder();
        for (Entry<StreamId, FixedLengthSliceInput> entry : streamsData.entrySet()) {
            streamsBuilder.put(entry.getKey(), new OrcInputStream(sourceName, entry.getValue(), compressionKind, bufferSize, systemMemoryUsage));
        }
        return streamsBuilder.build();
    }

    private Map<StreamId, ValueStream<?>> createValueStreams(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData, List<ColumnEncoding> columnEncodings)
    {
        ImmutableMap.Builder<StreamId, ValueStream<?>> valueStreams = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            ColumnEncodingKind columnEncoding = columnEncodings.get(stream.getColumn()).getColumnEncodingKind();

            // skip index and empty streams
            if (isIndexStream(stream) || stream.getLength() == 0) {
                continue;
            }

            OrcInputStream inputStream = streamsData.get(streamId);
            OrcTypeKind columnType = types.get(stream.getColumn()).getOrcTypeKind();

            valueStreams.put(streamId, ValueStreams.createValueStreams(streamId, inputStream, columnType, columnEncoding, stream.isUseVInts()));
        }
        return valueStreams.build();
    }

    public StreamSources createDictionaryStreamSources(Map<StreamId, Stream> streams, Map<StreamId, ValueStream<?>> valueStreams, List<ColumnEncoding> columnEncodings)
    {
        ImmutableMap.Builder<StreamId, StreamSource<?>> dictionaryStreamBuilder = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            int column = stream.getColumn();

            // only process dictionary streams
            ColumnEncodingKind columnEncoding = columnEncodings.get(column).getColumnEncodingKind();
            if (!isDictionary(stream, columnEncoding)) {
                continue;
            }

            // skip streams without data
            ValueStream<?> valueStream = valueStreams.get(streamId);
            if (valueStream == null) {
                continue;
            }

            OrcTypeKind columnType = types.get(stream.getColumn()).getOrcTypeKind();
            StreamCheckpoint streamCheckpoint = getDictionaryStreamCheckpoint(streamId, columnType, columnEncoding);

            StreamSource<?> streamSource = createCheckpointStreamSource(valueStream, streamCheckpoint);
            dictionaryStreamBuilder.put(streamId, streamSource);
        }
        return new StreamSources(dictionaryStreamBuilder.build());
    }

    private List<RowGroup> createRowGroups(
            int rowsInStripe,
            Map<StreamId, Stream> streams,
            Map<StreamId, ValueStream<?>> valueStreams,
            Map<Integer, List<RowGroupIndex>> columnIndexes,
            Set<Integer> selectedRowGroups,
            List<ColumnEncoding> encodings)
            throws InvalidCheckpointException
    {
        ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();

        for (int rowGroupId : selectedRowGroups) {
            Map<StreamId, StreamCheckpoint> checkpoints = getStreamCheckpoints(includedOrcColumns, types, compressionKind, rowGroupId, encodings, streams, columnIndexes);
            int rowOffset = rowGroupId * rowsInRowGroup;
            int rowsInGroup = Math.min(rowsInStripe - rowOffset, rowsInRowGroup);
            rowGroupBuilder.add(createRowGroup(rowGroupId, rowOffset, rowsInGroup, valueStreams, checkpoints));
        }

        return rowGroupBuilder.build();
    }

    public static RowGroup createRowGroup(int groupId, int rowOffset, int rowCount, Map<StreamId, ValueStream<?>> valueStreams, Map<StreamId, StreamCheckpoint> checkpoints)
    {
        ImmutableMap.Builder<StreamId, StreamSource<?>> builder = ImmutableMap.builder();
        for (Entry<StreamId, StreamCheckpoint> entry : checkpoints.entrySet()) {
            StreamId streamId = entry.getKey();
            StreamCheckpoint checkpoint = entry.getValue();

            // skip streams without data
            ValueStream<?> valueStream = valueStreams.get(streamId);
            if (valueStream == null) {
                continue;
            }

            builder.put(streamId, createCheckpointStreamSource(valueStream, checkpoint));
        }
        StreamSources rowGroupStreams = new StreamSources(builder.build());
        return new RowGroup(groupId, rowOffset, rowCount, rowGroupStreams);
    }

    public StripeFooter readStripeFooter(StripeInformation stripe, AbstractAggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        int tailLength = toIntExact(stripe.getFooterLength());

        // read the footer
        byte[] tailBuffer = new byte[tailLength];
        orcDataSource.readFully(offset, tailBuffer);
        try (InputStream inputStream = new OrcInputStream(orcDataSource.toString(), Slices.wrappedBuffer(tailBuffer).getInput(), compressionKind, bufferSize, systemMemoryUsage)) {
            return metadataReader.readStripeFooter(hiveWriterVersion, types, inputStream);
        }
    }

    private Map<Integer, List<HiveBloomFilter>> readBloomFilterIndexes(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData)
            throws IOException
    {
        ImmutableMap.Builder<Integer, List<HiveBloomFilter>> bloomFilters = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            Stream stream = entry.getValue();
            if (stream.getStreamKind() == BLOOM_FILTER) {
                OrcInputStream inputStream = streamsData.get(entry.getKey());
                bloomFilters.put(stream.getColumn(), metadataReader.readBloomFilterIndexes(inputStream));
            }
        }
        return bloomFilters.build();
    }

    private Map<Integer, List<RowGroupIndex>> readColumnIndexes(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData, Map<Integer, List<HiveBloomFilter>> bloomFilterIndexes)
            throws IOException
    {
        ImmutableMap.Builder<Integer, List<RowGroupIndex>> columnIndexes = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            Stream stream = entry.getValue();
            if (stream.getStreamKind() == ROW_INDEX) {
                OrcInputStream inputStream = streamsData.get(entry.getKey());
                List<HiveBloomFilter> bloomFilters = bloomFilterIndexes.get(stream.getColumn());
                List<RowGroupIndex> rowGroupIndexes = metadataReader.readRowIndexes(hiveWriterVersion, inputStream);
                if (bloomFilters != null && !bloomFilters.isEmpty()) {
                    ImmutableList.Builder<RowGroupIndex> newRowGroupIndexes = ImmutableList.builder();
                    for (int i = 0; i < rowGroupIndexes.size(); i++) {
                        RowGroupIndex rowGroupIndex = rowGroupIndexes.get(i);
                        ColumnStatistics columnStatistics = rowGroupIndex.getColumnStatistics()
                                .withBloomFilter(bloomFilters.get(i));
                        newRowGroupIndexes.add(new RowGroupIndex(rowGroupIndex.getPositions(), columnStatistics));
                    }
                    rowGroupIndexes = newRowGroupIndexes.build();
                }
                columnIndexes.put(stream.getColumn(), rowGroupIndexes);
            }
        }
        return columnIndexes.build();
    }

    private Set<Integer> selectRowGroups(StripeInformation stripe,  Map<Integer, List<RowGroupIndex>> columnIndexes)
            throws IOException
    {
        int rowsInStripe = toIntExact(stripe.getNumberOfRows());
        int groupsInStripe = ceil(rowsInStripe, rowsInRowGroup);

        ImmutableSet.Builder<Integer> selectedRowGroups = ImmutableSet.builder();
        int remainingRows = rowsInStripe;
        for (int rowGroup = 0; rowGroup < groupsInStripe; ++rowGroup) {
            int rows = Math.min(remainingRows, rowsInRowGroup);
            Map<Integer, ColumnStatistics> statistics = getRowGroupStatistics(types.get(0), columnIndexes, rowGroup);
            if (predicate.matches(rows, statistics)) {
                selectedRowGroups.add(rowGroup);
            }
            remainingRows -= rows;
        }
        return selectedRowGroups.build();
    }

    private static Map<Integer, ColumnStatistics> getRowGroupStatistics(OrcType rootStructType, Map<Integer, List<RowGroupIndex>> columnIndexes, int rowGroup)
    {
        requireNonNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        requireNonNull(columnIndexes, "columnIndexes is null");
        checkArgument(rowGroup >= 0, "rowGroup is negative");

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < rootStructType.getFieldCount(); ordinal++) {
            List<RowGroupIndex> rowGroupIndexes = columnIndexes.get(rootStructType.getFieldTypeIndex(ordinal));
            if (rowGroupIndexes != null) {
                statistics.put(ordinal, rowGroupIndexes.get(rowGroup).getColumnStatistics());
            }
        }
        return statistics.build();
    }

    private static boolean isIndexStream(Stream stream)
    {
        return stream.getStreamKind() == ROW_INDEX || stream.getStreamKind() == DICTIONARY_COUNT || stream.getStreamKind() == BLOOM_FILTER;
    }

    private static boolean isDictionary(Stream stream, ColumnEncodingKind columnEncoding)
    {
        return stream.getStreamKind() == DICTIONARY_DATA || (stream.getStreamKind() == LENGTH && (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2));
    }

    private static Map<StreamId, DiskRange> getDiskRanges(List<Stream> streams)
    {
        ImmutableMap.Builder<StreamId, DiskRange> streamDiskRanges = ImmutableMap.builder();
        long stripeOffset = 0;
        for (Stream stream : streams) {
            int streamLength = toIntExact(stream.getLength());
            streamDiskRanges.put(new StreamId(stream), new DiskRange(stripeOffset, streamLength));
            stripeOffset += streamLength;
        }
        return streamDiskRanges.build();
    }

    private static Set<Integer> getIncludedOrcColumns(List<OrcType> types, Set<Integer> includedColumns)
    {
        Set<Integer> includes = new LinkedHashSet<>();

        OrcType root = types.get(0);
        for (int includedColumn : includedColumns) {
            includeOrcColumnsRecursive(types, includes, root.getFieldTypeIndex(includedColumn));
        }

        return includes;
    }

    private static void includeOrcColumnsRecursive(List<OrcType> types, Set<Integer> result, int typeId)
    {
        result.add(typeId);
        OrcType type = types.get(typeId);
        int children = type.getFieldCount();
        for (int i = 0; i < children; ++i) {
            includeOrcColumnsRecursive(types, result, type.getFieldTypeIndex(i));
        }
    }

    /**
     * Ceiling of integer division
     */
    private static int ceil(int dividend, int divisor)
    {
        return ((dividend + divisor) - 1) / divisor;
    }
}
