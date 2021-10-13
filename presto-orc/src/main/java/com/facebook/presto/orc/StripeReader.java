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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.checkpoint.InvalidCheckpointException;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.DwrfSequenceEncoding;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.StripeEncryptionGroup;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.facebook.presto.orc.stream.ValueInputStream;
import com.facebook.presto.orc.stream.ValueInputStreamSource;
import com.facebook.presto.orc.stream.ValueStreams;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
import static com.facebook.presto.orc.checkpoint.Checkpoints.getDictionaryStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.Checkpoints.getStreamCheckpoints;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.DwrfMetadataReader.toStripeEncryptionGroup;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static com.facebook.presto.orc.metadata.Stream.StreamArea.INDEX;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.BLOOM_FILTER;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static com.facebook.presto.orc.stream.CheckpointInputStreamSource.createCheckpointStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class StripeReader
{
    private final OrcDataSource orcDataSource;
    private final Optional<OrcDecompressor> decompressor;
    private final List<OrcType> types;
    private final HiveWriterVersion hiveWriterVersion;
    private final Set<Integer> includedOrcColumns;
    private final int rowsInRowGroup;
    private final OrcPredicate predicate;
    private final MetadataReader metadataReader;
    private final Optional<OrcWriteValidation> writeValidation;
    private final StripeMetadataSource stripeMetadataSource;
    private final boolean cacheable;
    private final Multimap<Integer, Integer> dwrfEncryptionGroupColumns;
    private final RuntimeStats runtimeStats;

    public StripeReader(
            OrcDataSource orcDataSource,
            Optional<OrcDecompressor> decompressor,
            List<OrcType> types,
            Set<Integer> includedOrcColumns,
            int rowsInRowGroup,
            OrcPredicate predicate,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            Optional<OrcWriteValidation> writeValidation,
            StripeMetadataSource stripeMetadataSource,
            boolean cacheable,
            Map<Integer, Integer> dwrfEncryptionGroupMap,
            RuntimeStats runtimeStats)
    {
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.includedOrcColumns = requireNonNull(includedOrcColumns, "includedColumns is null");
        this.rowsInRowGroup = rowsInRowGroup;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.hiveWriterVersion = requireNonNull(hiveWriterVersion, "hiveWriterVersion is null");
        this.metadataReader = requireNonNull(metadataReader, "metadataReader is null");
        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.stripeMetadataSource = requireNonNull(stripeMetadataSource, "stripeMetadataSource is null");
        this.cacheable = cacheable;
        this.dwrfEncryptionGroupColumns = invertEncryptionGroupMap(requireNonNull(dwrfEncryptionGroupMap, "dwrfEncryptionGroupMap is null"));
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    private Multimap<Integer, Integer> invertEncryptionGroupMap(Map<Integer, Integer> dwrfEncryptionGroupMap)
    {
        ImmutableMultimap.Builder<Integer, Integer> invertedMapBuilder = ImmutableMultimap.builder();
        for (Entry<Integer, Integer> entry : dwrfEncryptionGroupMap.entrySet()) {
            invertedMapBuilder.put(entry.getValue(), entry.getKey());
        }
        for (int i = 0; i < types.size(); i++) {
            if (!dwrfEncryptionGroupMap.containsKey(i)) {
                invertedMapBuilder.put(-1, i);
            }
        }
        return invertedMapBuilder.build();
    }

    public Stripe readStripe(
            StripeInformation stripe,
            OrcAggregatedMemoryContext systemMemoryUsage,
            Optional<DwrfEncryptionInfo> decryptors,
            SharedBuffer sharedDecompressionBuffer)
            throws IOException
    {
        StripeId stripeId = new StripeId(orcDataSource.getId(), stripe.getOffset());

        // read the stripe footer
        StripeFooter stripeFooter = readStripeFooter(stripeId, stripe, systemMemoryUsage);

        // get streams for selected columns
        List<List<Stream>> allStreams = new ArrayList<>();
        allStreams.add(stripeFooter.getStreams());
        Map<StreamId, Stream> includedStreams = new HashMap<>();
        boolean hasRowGroupDictionary = addIncludedStreams(stripeFooter.getColumnEncodings(), stripeFooter.getStreams(), includedStreams);

        Map<Integer, ColumnEncoding> columnEncodings = new HashMap<>();

        Map<Integer, ColumnEncoding> stripeFooterEncodings = stripeFooter.getColumnEncodings();
        columnEncodings.putAll(stripeFooterEncodings);
        //  included columns may be encrypted
        if (decryptors.isPresent()) {
            List<Slice> encryptedEncryptionGroups = stripeFooter.getStripeEncryptionGroups();
            for (Integer groupId : decryptors.get().getEncryptorGroupIds()) {
                StripeEncryptionGroup stripeEncryptionGroup = getStripeEncryptionGroup(decryptors.get().getEncryptorByGroupId(groupId), encryptedEncryptionGroups.get(groupId), dwrfEncryptionGroupColumns.get(groupId), systemMemoryUsage);
                allStreams.add(stripeEncryptionGroup.getStreams());
                columnEncodings.putAll(stripeEncryptionGroup.getColumnEncodings());
                boolean encryptedHasRowGroupDictionary = addIncludedStreams(stripeEncryptionGroup.getColumnEncodings(), stripeEncryptionGroup.getStreams(), includedStreams);
                hasRowGroupDictionary = encryptedHasRowGroupDictionary || hasRowGroupDictionary;
            }
        }

        // handle stripes with more than one row group or a dictionary
        boolean invalidCheckPoint = false;
        if ((stripe.getNumberOfRows() > rowsInRowGroup) || hasRowGroupDictionary) {
            // determine ranges of the stripe to read
            Map<StreamId, DiskRange> diskRanges = getDiskRanges(allStreams);
            diskRanges = Maps.filterKeys(diskRanges, Predicates.in(includedStreams.keySet()));

            // read the file regions
            Map<StreamId, OrcInputStream> streamsData = readDiskRanges(stripeId, diskRanges, systemMemoryUsage, decryptors, sharedDecompressionBuffer);

            // read the bloom filter for each column
            Map<Integer, List<HiveBloomFilter>> bloomFilterIndexes = readBloomFilterIndexes(includedStreams, streamsData);

            // read the row index for each column
            Map<StreamId, List<RowGroupIndex>> columnIndexes = readColumnIndexes(includedStreams, streamsData, bloomFilterIndexes);
            if (writeValidation.isPresent()) {
                writeValidation.get().validateRowGroupStatistics(orcDataSource.getId(), stripe.getOffset(), columnIndexes);
            }

            // select the row groups matching the tuple domain
            Set<Integer> selectedRowGroups = selectRowGroups(stripe, columnIndexes);

            // if all row groups are skipped, return null
            if (selectedRowGroups.isEmpty()) {
                // set accounted memory usage to zero
                systemMemoryUsage.close();
                return null;
            }

            // value streams
            Map<StreamId, ValueInputStream<?>> valueStreams = createValueStreams(includedStreams, streamsData, columnEncodings);

            // build the dictionary streams
            InputStreamSources dictionaryStreamSources = createDictionaryStreamSources(includedStreams, valueStreams, columnEncodings);

            // build the row groups
            try {
                List<RowGroup> rowGroups = createRowGroups(
                        stripe.getNumberOfRows(),
                        includedStreams,
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
                    throw new OrcCorruptionException(e, orcDataSource.getId(), "Checkpoints are corrupt");
                }
                invalidCheckPoint = true;
            }
        }

        // stripe only has one row group and no dictionary
        ImmutableMap.Builder<StreamId, DiskRange> diskRangesBuilder = ImmutableMap.builder();
        for (Entry<StreamId, DiskRange> entry : getDiskRanges(allStreams).entrySet()) {
            StreamId streamId = entry.getKey();
            if (includedStreams.keySet().contains(streamId)) {
                diskRangesBuilder.put(entry);
            }
        }
        ImmutableMap<StreamId, DiskRange> diskRanges = diskRangesBuilder.build();

        // read the file regions
        Map<StreamId, OrcInputStream> streamsData = readDiskRanges(stripeId, diskRanges, systemMemoryUsage, decryptors, sharedDecompressionBuffer);

        long minAverageRowBytes = 0;
        for (Entry<StreamId, Stream> entry : includedStreams.entrySet()) {
            if (entry.getKey().getStreamKind() == ROW_INDEX) {
                List<RowGroupIndex> rowGroupIndexes = metadataReader.readRowIndexes(hiveWriterVersion, streamsData.get(entry.getKey()));
                checkState(rowGroupIndexes.size() == 1 || invalidCheckPoint, "expect a single row group or an invalid check point");
                long totalBytes = 0;
                long totalRows = 0;
                for (RowGroupIndex rowGroupIndex : rowGroupIndexes) {
                    ColumnStatistics columnStatistics = rowGroupIndex.getColumnStatistics();
                    if (columnStatistics.hasMinAverageValueSizeInBytes()) {
                        totalBytes += columnStatistics.getMinAverageValueSizeInBytes() * columnStatistics.getNumberOfValues();
                        totalRows += columnStatistics.getNumberOfValues();
                    }
                }
                if (totalRows > 0) {
                    minAverageRowBytes += totalBytes / totalRows;
                }
            }
        }

        // value streams
        Map<StreamId, ValueInputStream<?>> valueStreams = createValueStreams(includedStreams, streamsData, columnEncodings);

        // build the dictionary streams
        InputStreamSources dictionaryStreamSources = createDictionaryStreamSources(includedStreams, valueStreams, columnEncodings);

        // build the row group
        ImmutableMap.Builder<StreamId, InputStreamSource<?>> builder = ImmutableMap.builder();
        for (Entry<StreamId, ValueInputStream<?>> entry : valueStreams.entrySet()) {
            builder.put(entry.getKey(), new ValueInputStreamSource<>(entry.getValue()));
        }
        RowGroup rowGroup = new RowGroup(0, 0, stripe.getNumberOfRows(), minAverageRowBytes, new InputStreamSources(builder.build()));

        return new Stripe(stripe.getNumberOfRows(), columnEncodings, ImmutableList.of(rowGroup), dictionaryStreamSources);
    }

    private StripeEncryptionGroup getStripeEncryptionGroup(DwrfDataEncryptor decryptor, Slice encryptedGroup, Collection<Integer> columns, OrcAggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        OrcInputStream orcInputStream = new OrcInputStream(
                orcDataSource.getId(),
                // Memory is not accounted as the buffer is expected to be tiny and will be immediately discarded
                new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT),
                encryptedGroup.getInput(),
                decompressor,
                Optional.of(decryptor),
                systemMemoryUsage,
                encryptedGroup.length());
        return toStripeEncryptionGroup(orcDataSource.getId(), orcInputStream, types);
    }

    /**
     * Add streams that are in includedOrcColumns to the includedStreams map,
     * and return whether there were any rowGroupDictionaries
     */
    private boolean addIncludedStreams(Map<Integer, ColumnEncoding> columnEncodings, List<Stream> streams, Map<StreamId, Stream> includedStreams)
    {
        boolean hasRowGroupDictionary = false;
        for (Stream stream : streams) {
            if (includedOrcColumns.contains(stream.getColumn())) {
                includedStreams.put(new StreamId(stream), stream);

                if (stream.getStreamKind() == StreamKind.IN_DICTIONARY) {
                    ColumnEncoding columnEncoding = columnEncodings.get(stream.getColumn());

                    if (columnEncoding.getColumnEncodingKind() == DICTIONARY) {
                        hasRowGroupDictionary = true;
                    }

                    Optional<SortedMap<Integer, DwrfSequenceEncoding>> additionalSequenceEncodings = columnEncoding.getAdditionalSequenceEncodings();
                    if (additionalSequenceEncodings.isPresent()
                            && additionalSequenceEncodings.get().values().stream()
                            .map(DwrfSequenceEncoding::getValueEncoding)
                            .anyMatch(encoding -> encoding.getColumnEncodingKind() == DICTIONARY)) {
                        hasRowGroupDictionary = true;
                    }
                }
            }
        }
        return hasRowGroupDictionary;
    }

    private Map<StreamId, OrcInputStream> readDiskRanges(
            StripeId stripeId,
            Map<StreamId, DiskRange> diskRanges,
            OrcAggregatedMemoryContext systemMemoryUsage,
            Optional<DwrfEncryptionInfo> decryptors,
            SharedBuffer sharedDecompressionBuffer)
            throws IOException
    {
        //
        // Note: this code does not use the Java 8 stream APIs to avoid any extra object allocation
        //

        // read ranges
        Map<StreamId, OrcDataSourceInput> streamsData = stripeMetadataSource.getInputs(orcDataSource, stripeId, diskRanges, cacheable);

        // transform streams to OrcInputStream
        ImmutableMap.Builder<StreamId, OrcInputStream> streamsBuilder = ImmutableMap.builder();
        for (Entry<StreamId, OrcDataSourceInput> entry : streamsData.entrySet()) {
            OrcDataSourceInput sourceInput = entry.getValue();
            Optional<DwrfDataEncryptor> dwrfDecryptor = createDwrfDecryptor(entry.getKey(), decryptors);
            streamsBuilder.put(entry.getKey(), new OrcInputStream(
                    orcDataSource.getId(),
                    sharedDecompressionBuffer,
                    sourceInput.getInput(),
                    decompressor,
                    dwrfDecryptor,
                    systemMemoryUsage,
                    sourceInput.getRetainedSizeInBytes()));
        }
        return streamsBuilder.build();
    }

    private Optional<DwrfDataEncryptor> createDwrfDecryptor(StreamId id, Optional<DwrfEncryptionInfo> decryptors)
    {
        if (!decryptors.isPresent()) {
            return Optional.empty();
        }
        return decryptors.get().getEncryptorByNodeId(id.getColumn());
    }

    private Map<StreamId, ValueInputStream<?>> createValueStreams(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData, Map<Integer, ColumnEncoding> columnEncodings)
    {
        ImmutableMap.Builder<StreamId, ValueInputStream<?>> valueStreams = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            ColumnEncodingKind columnEncoding = columnEncodings.get(stream.getColumn())
                    .getColumnEncoding(stream.getSequence())
                    .getColumnEncodingKind();

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

    public InputStreamSources createDictionaryStreamSources(Map<StreamId, Stream> streams, Map<StreamId, ValueInputStream<?>> valueStreams, Map<Integer, ColumnEncoding> columnEncodings)
    {
        ImmutableMap.Builder<StreamId, InputStreamSource<?>> dictionaryStreamBuilder = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            int column = stream.getColumn();

            // only process dictionary streams
            ColumnEncodingKind columnEncoding = columnEncodings.get(column)
                    .getColumnEncoding(stream.getSequence())
                    .getColumnEncodingKind();
            if (!isDictionary(stream, columnEncoding)) {
                continue;
            }

            // skip streams without data
            ValueInputStream<?> valueStream = valueStreams.get(streamId);
            if (valueStream == null) {
                continue;
            }

            OrcTypeKind columnType = types.get(stream.getColumn()).getOrcTypeKind();
            StreamCheckpoint streamCheckpoint = getDictionaryStreamCheckpoint(streamId, columnType, columnEncoding);

            InputStreamSource<?> streamSource = createCheckpointStreamSource(valueStream, streamCheckpoint);
            dictionaryStreamBuilder.put(streamId, streamSource);
        }
        return new InputStreamSources(dictionaryStreamBuilder.build());
    }

    private List<RowGroup> createRowGroups(
            long rowsInStripe,
            Map<StreamId, Stream> streams,
            Map<StreamId, ValueInputStream<?>> valueStreams,
            Map<StreamId, List<RowGroupIndex>> columnIndexes,
            Set<Integer> selectedRowGroups,
            Map<Integer, ColumnEncoding> encodings)
            throws InvalidCheckpointException
    {
        ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();

        for (int rowGroupId : selectedRowGroups) {
            Map<StreamId, StreamCheckpoint> checkpoints = getStreamCheckpoints(includedOrcColumns, types, decompressor.isPresent(), rowGroupId, encodings, streams, columnIndexes);
            long minAverageRowBytes = columnIndexes
                    .entrySet()
                    .stream()
                    .mapToLong(e -> e.getValue()
                            .get(rowGroupId)
                            .getColumnStatistics()
                            .getMinAverageValueSizeInBytes())
                    .sum();
            rowGroupBuilder.add(createRowGroup(rowGroupId, rowsInStripe, rowsInRowGroup, minAverageRowBytes, valueStreams, checkpoints));
        }

        return rowGroupBuilder.build();
    }

    @VisibleForTesting
    static RowGroup createRowGroup(int groupId, long rowsInStripe, long rowsInRowGroup, long minAverageRowBytes, Map<StreamId, ValueInputStream<?>> valueStreams, Map<StreamId, StreamCheckpoint> checkpoints)
    {
        long rowOffset = multiplyExact(groupId, rowsInRowGroup);
        int rowsInGroup = toIntExact(Math.min(rowsInStripe - rowOffset, rowsInRowGroup));
        ImmutableMap.Builder<StreamId, InputStreamSource<?>> builder = ImmutableMap.builder();
        for (Entry<StreamId, StreamCheckpoint> entry : checkpoints.entrySet()) {
            StreamId streamId = entry.getKey();
            StreamCheckpoint checkpoint = entry.getValue();

            // skip streams without data
            ValueInputStream<?> valueStream = valueStreams.get(streamId);
            if (valueStream == null) {
                continue;
            }

            builder.put(streamId, createCheckpointStreamSource(valueStream, checkpoint));
        }
        InputStreamSources rowGroupStreams = new InputStreamSources(builder.build());
        return new RowGroup(groupId, rowOffset, rowsInGroup, minAverageRowBytes, rowGroupStreams);
    }

    public StripeFooter readStripeFooter(StripeId stripeId, StripeInformation stripe, OrcAggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        long footerOffset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        int footerLength = toIntExact(stripe.getFooterLength());

        // read the footer
        Slice footerSlice = stripeMetadataSource.getStripeFooterSlice(orcDataSource, stripeId, footerOffset, footerLength, cacheable);
        try (InputStream inputStream = new OrcInputStream(
                orcDataSource.getId(),
                // Memory is not accounted as the buffer is expected to be tiny and will be immediately discarded
                new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT),
                footerSlice.getInput(),
                decompressor,
                Optional.empty(),
                systemMemoryUsage,
                footerLength)) {
            return metadataReader.readStripeFooter(orcDataSource.getId(), types, inputStream);
        }
    }

    static boolean isIndexStream(Stream stream)
    {
        return stream.getStreamKind().getStreamArea() == INDEX;
    }

    private Map<Integer, List<HiveBloomFilter>> readBloomFilterIndexes(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData)
            throws IOException
    {
        ImmutableMap.Builder<Integer, List<HiveBloomFilter>> bloomFilters = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            Stream stream = entry.getValue();
            if (stream.getStreamKind() == BLOOM_FILTER) {
                OrcInputStream inputStream = streamsData.get(entry.getKey());
                bloomFilters.put(entry.getKey().getColumn(), metadataReader.readBloomFilterIndexes(inputStream));
            }
            // TODO: add support for BLOOM_FILTER_UTF8
        }
        return bloomFilters.build();
    }

    private Map<StreamId, List<RowGroupIndex>> readColumnIndexes(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData, Map<Integer, List<HiveBloomFilter>> bloomFilterIndexes)
            throws IOException
    {
        ImmutableMap.Builder<StreamId, List<RowGroupIndex>> columnIndexes = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            Stream stream = entry.getValue();
            if (stream.getStreamKind() == ROW_INDEX) {
                OrcInputStream inputStream = streamsData.get(entry.getKey());
                List<HiveBloomFilter> bloomFilters = bloomFilterIndexes.get(entry.getKey().getColumn());
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
                columnIndexes.put(entry.getKey(), rowGroupIndexes);
            }
        }
        return columnIndexes.build();
    }

    private Set<Integer> selectRowGroups(StripeInformation stripe, Map<StreamId, List<RowGroupIndex>> columnIndexes)
    {
        long rowsInStripe = stripe.getNumberOfRows();
        int groupsInStripe = ceil(rowsInStripe, rowsInRowGroup);

        ImmutableSet.Builder<Integer> selectedRowGroups = ImmutableSet.builder();
        long remainingRows = rowsInStripe;
        for (int rowGroup = 0; rowGroup < groupsInStripe; ++rowGroup) {
            int rows = toIntExact(Math.min(remainingRows, rowsInRowGroup));
            Map<Integer, ColumnStatistics> statistics = getRowGroupStatistics(types.get(0), columnIndexes, rowGroup);
            if (predicate.matches(rows, statistics)) {
                selectedRowGroups.add(rowGroup);
            }
            remainingRows -= rows;
        }
        return selectedRowGroups.build();
    }

    private static Map<Integer, ColumnStatistics> getRowGroupStatistics(OrcType rootStructType, Map<StreamId, List<RowGroupIndex>> columnIndexes, int rowGroup)
    {
        requireNonNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == STRUCT);
        requireNonNull(columnIndexes, "columnIndexes is null");
        checkArgument(rowGroup >= 0, "rowGroup is negative");

        Map<Integer, List<ColumnStatistics>> groupedColumnStatistics = new HashMap<>();
        for (Entry<StreamId, List<RowGroupIndex>> entry : columnIndexes.entrySet()) {
            if (!entry.getValue().isEmpty() && entry.getValue().get(rowGroup) != null) {
                groupedColumnStatistics.computeIfAbsent(entry.getKey().getColumn(), key -> new ArrayList<>())
                        .add(entry.getValue().get(rowGroup).getColumnStatistics());
            }
        }

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < rootStructType.getFieldCount(); ordinal++) {
            List<ColumnStatistics> columnStatistics = groupedColumnStatistics.get(rootStructType.getFieldTypeIndex(ordinal));
            if (columnStatistics != null) {
                if (columnStatistics.size() == 1) {
                    statistics.put(ordinal, getOnlyElement(columnStatistics));
                }
                else {
                    // Merge statistics from different streams
                    // This can happen if map is represented as struct (DWRF only)
                    statistics.put(ordinal, mergeColumnStatistics(columnStatistics));
                }
            }
        }
        return statistics.build();
    }

    private static boolean isDictionary(Stream stream, ColumnEncodingKind columnEncoding)
    {
        return stream.getStreamKind() == DICTIONARY_DATA || (stream.getStreamKind() == LENGTH && (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2));
    }

    @VisibleForTesting
    public static Map<StreamId, DiskRange> getDiskRanges(List<List<Stream>> streams)
    {
        ImmutableMap.Builder<StreamId, DiskRange> streamDiskRanges = ImmutableMap.builder();
        for (List<Stream> groupStreams : streams) {
            long stripeOffset = 0;
            for (Stream stream : groupStreams) {
                int streamLength = toIntExact(stream.getLength());
                if (stream.getOffset().isPresent()) {
                    stripeOffset = stream.getOffset().get();
                }
                // ignore zero byte streams
                if (streamLength > 0) {
                    streamDiskRanges.put(new StreamId(stream), new DiskRange(stripeOffset, streamLength));
                }
                stripeOffset += streamLength;
            }
        }
        return streamDiskRanges.build();
    }

    /**
     * Ceiling of integer division
     */
    private static int ceil(long dividend, int divisor)
    {
        long ceil = ((dividend + divisor) - 1) / divisor;
        return toIntExact(ceil);
    }

    public static class StripeId
    {
        private final OrcDataSourceId sourceId;
        private final long offset;

        public StripeId(OrcDataSourceId sourceId, long offset)
        {
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.offset = offset;
        }

        public OrcDataSourceId getSourceId()
        {
            return sourceId;
        }

        public long getOffset()
        {
            return offset;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StripeId stripeId = (StripeId) o;
            return offset == stripeId.offset &&
                    Objects.equals(sourceId, stripeId.sourceId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceId, offset);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("sourceId", sourceId)
                    .add("offset", offset)
                    .toString();
        }
    }

    public static class StripeStreamId
    {
        private final StripeId stripeId;
        private final StreamId streamId;

        public StripeStreamId(StripeId stripeId, StreamId streamId)
        {
            this.stripeId = requireNonNull(stripeId, "stripeId is null");
            this.streamId = requireNonNull(streamId, "streamId is null");
        }

        public StreamId getStreamId()
        {
            return streamId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StripeStreamId that = (StripeStreamId) o;
            return Objects.equals(stripeId, that.stripeId) &&
                    Objects.equals(streamId, that.streamId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stripeId, streamId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("stripeId", stripeId)
                    .add("streamId", streamId)
                    .toString();
        }
    }
}
