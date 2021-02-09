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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationBuilder;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.orc.WriterStats.FlushReason;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.Metadata;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeEncryptionGroup;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.facebook.presto.orc.writer.ColumnWriter;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.io.DataOutput.createDataOutput;
import static com.facebook.presto.orc.DwrfEncryptionInfo.UNENCRYPTED;
import static com.facebook.presto.orc.DwrfEncryptionInfo.createNodeToGroupMap;
import static com.facebook.presto.orc.OrcReader.validateFile;
import static com.facebook.presto.orc.WriterStats.FlushReason.CLOSED;
import static com.facebook.presto.orc.WriterStats.FlushReason.DICTIONARY_FULL;
import static com.facebook.presto.orc.WriterStats.FlushReason.MAX_BYTES;
import static com.facebook.presto.orc.WriterStats.FlushReason.MAX_ROWS;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toFileStatistics;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toStripeEncryptionGroup;
import static com.facebook.presto.orc.metadata.PostScript.MAGIC;
import static com.facebook.presto.orc.writer.ColumnWriters.createColumnWriter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Integer.min;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcWriter.class).instanceSize();
    private static final Logger log = Logger.get(OrcWriter.class);

    static final String PRESTO_ORC_WRITER_VERSION_METADATA_KEY = "presto.writer.version";
    static final String PRESTO_ORC_WRITER_VERSION;
    private final WriterStats stats;

    static {
        String version = OrcWriter.class.getPackage().getImplementationVersion();
        PRESTO_ORC_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final DataSink dataSink;
    private final List<Type> types;
    private final OrcEncoding orcEncoding;
    private final CompressionParameters compressionParameters;
    private final int stripeMinBytes;
    private final int stripeMaxBytes;
    private final int chunkMaxLogicalBytes;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final Map<String, String> userMetadata;
    private final CompressedMetadataWriter metadataWriter;
    private final DateTimeZone hiveStorageTimeZone;

    private final DwrfEncryptionProvider dwrfEncryptionProvider;
    private final DwrfEncryptionInfo dwrfEncryptionInfo;
    private final Optional<DwrfWriterEncryption> dwrfWriterEncryption;

    private final List<ClosedStripe> closedStripes = new ArrayList<>();
    private final List<OrcType> orcTypes;

    private final List<ColumnWriter> columnWriters;
    private final int dictionaryMaxMemoryBytes;
    private final DictionaryCompressionOptimizer dictionaryCompressionOptimizer;
    private int stripeRowCount;
    private int rowGroupRowCount;
    private int bufferedBytes;
    private long columnWritersRetainedBytes;
    private long closedStripesRetainedBytes;
    private long previouslyRecordedSizeInBytes;
    private boolean closed;

    @Nullable
    private final OrcWriteValidation.OrcWriteValidationBuilder validationBuilder;

    public OrcWriter(
            DataSink dataSink,
            List<String> columnNames,
            List<Type> types,
            OrcEncoding orcEncoding,
            CompressionKind compressionKind,
            Optional<DwrfWriterEncryption> encryption,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            OrcWriterOptions options,
            Map<String, String> userMetadata,
            DateTimeZone hiveStorageTimeZone,
            boolean validate,
            OrcWriteValidationMode validationMode,
            WriterStats stats)
    {
        this.validationBuilder = validate ? new OrcWriteValidation.OrcWriteValidationBuilder(validationMode, types).setStringStatisticsLimitInBytes(toIntExact(options.getMaxStringStatisticsLimit().toBytes())) : null;

        this.dataSink = requireNonNull(dataSink, "dataSink is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.orcEncoding = requireNonNull(orcEncoding, "orcEncoding is null");

        requireNonNull(compressionKind, "compressionKind is null");
        int maxCompressionBufferSize = toIntExact(options.getMaxCompressionBufferSize().toBytes());
        this.compressionParameters = new CompressionParameters(compressionKind, options.getCompressionLevel(), maxCompressionBufferSize);
        recordValidation(validation -> validation.setCompression(compressionKind));

        requireNonNull(options, "options is null");
        checkArgument(options.getStripeMaxSize().compareTo(options.getStripeMinSize()) >= 0, "stripeMaxSize must be greater than stripeMinSize");
        this.stripeMinBytes = toIntExact(requireNonNull(options.getStripeMinSize(), "stripeMinSize is null").toBytes());
        this.stripeMaxBytes = toIntExact(requireNonNull(options.getStripeMaxSize(), "stripeMaxSize is null").toBytes());
        this.chunkMaxLogicalBytes = max(1, stripeMaxBytes / 2);
        this.stripeMaxRowCount = options.getStripeMaxRowCount();
        this.rowGroupMaxRowCount = options.getRowGroupMaxRowCount();
        recordValidation(validation -> validation.setRowGroupMaxRowCount(rowGroupMaxRowCount));

        this.userMetadata = ImmutableMap.<String, String>builder()
                .putAll(requireNonNull(userMetadata, "userMetadata is null"))
                .put(PRESTO_ORC_WRITER_VERSION_METADATA_KEY, PRESTO_ORC_WRITER_VERSION)
                .build();
        this.metadataWriter = new CompressedMetadataWriter(orcEncoding.createMetadataWriter(), compressionParameters, Optional.empty());
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.stats = requireNonNull(stats, "stats is null");

        requireNonNull(columnNames, "columnNames is null");
        this.orcTypes = OrcType.createOrcRowType(0, columnNames, types);
        recordValidation(validation -> validation.setColumnNames(columnNames));

        dwrfWriterEncryption = requireNonNull(encryption, "encryption is null");
        this.dwrfEncryptionProvider = requireNonNull(dwrfEncryptionProvider, "dwrfEncryptionProvider is null");
        if (dwrfWriterEncryption.isPresent()) {
            List<WriterEncryptionGroup> writerEncryptionGroups = dwrfWriterEncryption.get().getWriterEncryptionGroups();
            Map<Integer, Integer> nodeToGroupMap = createNodeToGroupMap(
                    writerEncryptionGroups
                            .stream()
                            .map(WriterEncryptionGroup::getNodes)
                            .collect(toImmutableList()),
                    orcTypes);
            EncryptionLibrary encryptionLibrary = dwrfEncryptionProvider.getEncryptionLibrary(dwrfWriterEncryption.get().getKeyProvider());
            List<byte[]> dataEncryptionKeys = writerEncryptionGroups.stream()
                    .map(group -> encryptionLibrary.generateDataEncryptionKey(group.getIntermediateKeyMetadata().getBytes()))
                    .collect(toImmutableList());
            Map<Integer, DwrfDataEncryptor> dwrfEncryptors = IntStream.range(0, writerEncryptionGroups.size())
                    .boxed()
                    .collect(toImmutableMap(
                            groupId -> groupId,
                            groupId -> new DwrfDataEncryptor(dataEncryptionKeys.get(groupId), encryptionLibrary)));

            List<byte[]> encryptedKeyMetadatas = IntStream.range(0, writerEncryptionGroups.size())
                    .boxed()
                    .map(groupId -> encryptionLibrary.encryptKey(
                            writerEncryptionGroups.get(groupId).getIntermediateKeyMetadata().getBytes(),
                            dataEncryptionKeys.get(groupId),
                            0,
                            dataEncryptionKeys.get(groupId).length))
                    .collect(toImmutableList());
            this.dwrfEncryptionInfo = new DwrfEncryptionInfo(dwrfEncryptors, encryptedKeyMetadatas, nodeToGroupMap);
        }
        else {
            this.dwrfEncryptionInfo = UNENCRYPTED;
        }

        // create column writers
        OrcType rootType = orcTypes.get(0);
        checkArgument(rootType.getFieldCount() == types.size());
        ImmutableList.Builder<ColumnWriter> columnWriters = ImmutableList.builder();
        ImmutableSet.Builder<SliceDictionaryColumnWriter> sliceColumnWriters = ImmutableSet.builder();
        for (int fieldId = 0; fieldId < types.size(); fieldId++) {
            int fieldColumnIndex = rootType.getFieldTypeIndex(fieldId);
            Type fieldType = types.get(fieldId);
            ColumnWriter columnWriter = createColumnWriter(
                    fieldColumnIndex,
                    orcTypes,
                    fieldType,
                    compressionParameters,
                    orcEncoding,
                    hiveStorageTimeZone,
                    options.getMaxStringStatisticsLimit(),
                    dwrfEncryptionInfo,
                    orcEncoding.createMetadataWriter());
            columnWriters.add(columnWriter);

            if (columnWriter instanceof SliceDictionaryColumnWriter) {
                sliceColumnWriters.add((SliceDictionaryColumnWriter) columnWriter);
            }
            else {
                for (ColumnWriter nestedColumnWriter : columnWriter.getNestedColumnWriters()) {
                    if (nestedColumnWriter instanceof SliceDictionaryColumnWriter) {
                        sliceColumnWriters.add((SliceDictionaryColumnWriter) nestedColumnWriter);
                    }
                }
            }
        }
        this.columnWriters = columnWriters.build();
        this.dictionaryMaxMemoryBytes = toIntExact(
                requireNonNull(options.getDictionaryMaxMemory(), "dictionaryMaxMemory is null").toBytes());
        this.dictionaryCompressionOptimizer = new DictionaryCompressionOptimizer(
                sliceColumnWriters.build(),
                stripeMinBytes,
                stripeMaxBytes,
                stripeMaxRowCount,
                dictionaryMaxMemoryBytes);

        for (Entry<String, String> entry : this.userMetadata.entrySet()) {
            recordValidation(validation -> validation.addMetadataProperty(entry.getKey(), utf8Slice(entry.getValue())));
        }

        this.previouslyRecordedSizeInBytes = getRetainedBytes();
        stats.updateSizeInBytes(previouslyRecordedSizeInBytes);
    }

    /**
     * Number of bytes already flushed to the data sink.
     */
    public long getWrittenBytes()
    {
        return dataSink.size();
    }

    /**
     * Number of pending bytes not yet flushed.
     */
    public int getBufferedBytes()
    {
        return bufferedBytes;
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                columnWritersRetainedBytes +
                closedStripesRetainedBytes +
                dataSink.getRetainedSizeInBytes() +
                (validationBuilder == null ? 0 : validationBuilder.getRetainedSize());
    }

    public void write(Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        checkArgument(page.getChannelCount() == columnWriters.size());

        if (validationBuilder != null) {
            validationBuilder.addPage(page);
        }

        // avoid chunk with huge logical size
        double averageLogicalSizePerRow = (double) page.getApproximateLogicalSizeInBytes() / page.getPositionCount();
        int maxChunkRowCount = max(1, (int) (chunkMaxLogicalBytes / max(1, averageLogicalSizePerRow)));

        while (page != null) {
            // logical size and row group boundaries
            int chunkRows = min(maxChunkRowCount, min(rowGroupMaxRowCount - rowGroupRowCount, stripeMaxRowCount - stripeRowCount));

            // align page to max size per chunk
            chunkRows = min(page.getPositionCount(), chunkRows);

            Page chunk = page.getRegion(0, chunkRows);

            if (chunkRows < page.getPositionCount()) {
                page = page.getRegion(chunkRows, page.getPositionCount() - chunkRows);
            }
            else {
                page = null;
            }

            writeChunk(chunk);
        }

        long recordedSizeInBytes = getRetainedBytes();
        stats.updateSizeInBytes(recordedSizeInBytes - previouslyRecordedSizeInBytes);
        previouslyRecordedSizeInBytes = recordedSizeInBytes;
    }

    private void writeChunk(Page chunk)
            throws IOException
    {
        if (rowGroupRowCount == 0) {
            columnWriters.forEach(ColumnWriter::beginRowGroup);
        }

        // write chunks
        bufferedBytes = 0;
        for (int channel = 0; channel < chunk.getChannelCount(); channel++) {
            ColumnWriter writer = columnWriters.get(channel);
            writer.writeBlock(chunk.getBlock(channel));
            bufferedBytes += writer.getBufferedBytes();
        }

        // update stats
        rowGroupRowCount += chunk.getPositionCount();
        checkState(rowGroupRowCount <= rowGroupMaxRowCount);
        stripeRowCount += chunk.getPositionCount();

        // record checkpoint if necessary
        if (rowGroupRowCount == rowGroupMaxRowCount) {
            finishRowGroup();
        }

        // convert dictionary encoded columns to direct if dictionary memory usage exceeded
        dictionaryCompressionOptimizer.optimize(bufferedBytes, stripeRowCount);

        // flush stripe if necessary
        bufferedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum());
        if (stripeRowCount == stripeMaxRowCount) {
            flushStripe(MAX_ROWS);
        }
        else if (bufferedBytes > stripeMaxBytes) {
            flushStripe(MAX_BYTES);
        }
        else if (dictionaryCompressionOptimizer.isFull(bufferedBytes)) {
            flushStripe(DICTIONARY_FULL);
        }

        columnWritersRetainedBytes = columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum();
    }

    private void finishRowGroup()
    {
        Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.finishRowGroup()));
        recordValidation(validation -> validation.addRowGroupStatistics(columnStatistics));
        rowGroupRowCount = 0;
    }

    private void flushStripe(FlushReason flushReason)
            throws IOException
    {
        List<DataOutput> outputData = new ArrayList<>();
        long stripeStartOffset = dataSink.size();
        // add header to first stripe (this is not required but nice to have)
        if (closedStripes.isEmpty()) {
            outputData.add(createDataOutput(MAGIC));
            stripeStartOffset += MAGIC.length();
        }

        flushColumnWriters(flushReason);
        try {
            // add stripe data
            outputData.addAll(bufferStripeData(stripeStartOffset, flushReason));
            // if the file is being closed, add the file footer
            if (flushReason == CLOSED) {
                outputData.addAll(bufferFileFooter());
            }

            // write all data
            dataSink.write(outputData);
        }
        finally {
            // open next stripe
            columnWriters.forEach(ColumnWriter::reset);
            dictionaryCompressionOptimizer.reset();
            rowGroupRowCount = 0;
            stripeRowCount = 0;
            bufferedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum());
        }
    }

    private void flushColumnWriters(FlushReason flushReason)
    {
        if (stripeRowCount == 0) {
            verify(flushReason == CLOSED, "An empty stripe is not allowed");
        }
        else {
            if (rowGroupRowCount > 0) {
                finishRowGroup();
            }

            // convert any dictionary encoded column with a low compression ratio to direct
            dictionaryCompressionOptimizer.finalOptimize(bufferedBytes);
        }

        columnWriters.forEach(ColumnWriter::close);
    }

    /**
     * Collect the data for for the stripe.  This is not the actual data, but
     * instead are functions that know how to write the data.
     */
    private List<DataOutput> bufferStripeData(long stripeStartOffset, FlushReason flushReason)
            throws IOException
    {
        if (stripeRowCount == 0) {
            return ImmutableList.of();
        }

        List<DataOutput> outputData = new ArrayList<>();
        List<Stream> unencryptedStreams = new ArrayList<>(columnWriters.size() * 3);
        Multimap<Integer, Stream> encryptedStreams = ArrayListMultimap.create();

        // get index streams
        long indexLength = 0;
        long offset = 0;
        int previousEncryptionGroup = -1;
        for (ColumnWriter columnWriter : columnWriters) {
            for (StreamDataOutput indexStream : columnWriter.getIndexStreams()) {
                // The ordering is critical because the stream only contain a length with no offset.
                // if the previous stream was part of a different encryption group, need to specify an offset so we know the column order
                outputData.add(indexStream);
                Optional<Integer> encryptionGroup = dwrfEncryptionInfo.getGroupByNodeId(indexStream.getStream().getColumn());
                if (encryptionGroup.isPresent()) {
                    Stream stream = previousEncryptionGroup == encryptionGroup.get() ? indexStream.getStream() : indexStream.getStream().withOffset(offset);
                    encryptedStreams.put(encryptionGroup.get(), stream);
                    previousEncryptionGroup = encryptionGroup.get();
                }
                else {
                    Stream stream = previousEncryptionGroup == -1 ? indexStream.getStream() : indexStream.getStream().withOffset(offset);
                    unencryptedStreams.add(stream);
                    previousEncryptionGroup = -1;
                }
                offset += indexStream.size();
                indexLength += indexStream.size();
            }
        }

        // data streams (sorted by size)
        long dataLength = 0;
        List<StreamDataOutput> dataStreams = new ArrayList<>(columnWriters.size() * 2);
        for (ColumnWriter columnWriter : columnWriters) {
            List<StreamDataOutput> streams = columnWriter.getDataStreams();
            dataStreams.addAll(streams);
            dataLength += streams.stream()
                    .mapToLong(StreamDataOutput::size)
                    .sum();
        }
        Collections.sort(dataStreams);

        // add data streams
        for (StreamDataOutput dataStream : dataStreams) {
            // The ordering is critical because the stream only contains a length with no offset.
            // if the previous stream was part of a different encryption group, need to specify an offset so we know the column order
            outputData.add(dataStream);
            Optional<Integer> encryptionGroup = dwrfEncryptionInfo.getGroupByNodeId(dataStream.getStream().getColumn());
            if (encryptionGroup.isPresent()) {
                Stream stream = previousEncryptionGroup == encryptionGroup.get() ? dataStream.getStream() : dataStream.getStream().withOffset(offset);
                encryptedStreams.put(encryptionGroup.get(), stream);
                previousEncryptionGroup = encryptionGroup.get();
            }
            else {
                Stream stream = previousEncryptionGroup == -1 ? dataStream.getStream() : dataStream.getStream().withOffset(offset);
                unencryptedStreams.add(stream);
                previousEncryptionGroup = -1;
            }
            offset += dataStream.size();
        }

        Map<Integer, ColumnEncoding> columnEncodings = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnEncodings.putAll(columnWriter.getColumnEncodings()));

        Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.getColumnStripeStatistics()));

        // the 0th column is a struct column for the whole row
        columnEncodings.put(0, new ColumnEncoding(DIRECT, 0));
        columnStatistics.put(0, new ColumnStatistics((long) stripeRowCount, 0, null, null, null, null, null, null, null, null));

        Map<Integer, ColumnEncoding> unencryptedColumnEncodings = columnEncodings.entrySet().stream()
                .filter(entry -> !dwrfEncryptionInfo.getGroupByNodeId(entry.getKey()).isPresent())
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        Map<Integer, ColumnEncoding> encryptedColumnEncodings = columnEncodings.entrySet().stream()
                .filter(entry -> dwrfEncryptionInfo.getGroupByNodeId(entry.getKey()).isPresent())
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        List<Slice> encryptedGroups = createEncryptedGroups(encryptedStreams, encryptedColumnEncodings);

        StripeFooter stripeFooter = new StripeFooter(unencryptedStreams, unencryptedColumnEncodings, encryptedGroups);
        Slice footer = metadataWriter.writeStripeFooter(stripeFooter);
        outputData.add(createDataOutput(footer));

        // create final stripe statistics
        StripeStatistics statistics = new StripeStatistics(toDenseList(columnStatistics, orcTypes.size()));

        recordValidation(validation -> validation.addStripeStatistics(stripeStartOffset, statistics));

        StripeInformation stripeInformation = new StripeInformation(stripeRowCount, stripeStartOffset, indexLength, dataLength, footer.length(), dwrfEncryptionInfo.getEncryptedKeyMetadatas());
        ClosedStripe closedStripe = new ClosedStripe(stripeInformation, statistics);
        closedStripes.add(closedStripe);
        closedStripesRetainedBytes += closedStripe.getRetainedSizeInBytes();

        recordValidation(validation -> validation.addStripe(stripeInformation.getNumberOfRows()));
        stats.recordStripeWritten(stripeMinBytes, stripeMaxBytes, dictionaryMaxMemoryBytes, flushReason, dictionaryCompressionOptimizer.getDictionaryMemoryBytes(), stripeInformation);

        return outputData;
    }

    private List<Slice> createEncryptedGroups(Multimap<Integer, Stream> encryptedStreams, Map<Integer, ColumnEncoding> encryptedColumnEncodings)
            throws IOException
    {
        ImmutableList.Builder<Slice> encryptedGroups = ImmutableList.builder();
        for (int i = 0; i < encryptedStreams.keySet().size(); i++) {
            int groupId = i;
            Map<Integer, ColumnEncoding> groupColumnEncodings = encryptedColumnEncodings.entrySet().stream()
                    .filter(entry -> dwrfEncryptionInfo.getGroupByNodeId(entry.getKey()).orElseThrow(() -> new VerifyError("missing group for encryptedColumn")) == groupId)
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));
            DwrfDataEncryptor dwrfDataEncryptor = dwrfEncryptionInfo.getEncryptorByGroupId(i);
            OrcOutputBuffer buffer = new OrcOutputBuffer(compressionParameters, Optional.of(dwrfDataEncryptor));
            toStripeEncryptionGroup(
                    new StripeEncryptionGroup(
                            ImmutableList.copyOf(encryptedStreams.get(i)),
                            groupColumnEncodings))
                    .writeTo(buffer);
            buffer.close();
            DynamicSliceOutput output = new DynamicSliceOutput(toIntExact(buffer.getOutputDataSize()));
            buffer.writeDataTo(output);
            encryptedGroups.add(output.slice());
        }
        return encryptedGroups.build();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        stats.updateSizeInBytes(-previouslyRecordedSizeInBytes);
        previouslyRecordedSizeInBytes = 0;

        flushStripe(CLOSED);

        dataSink.close();
    }

    /**
     * Collect the data for for the file footer.  This is not the actual data, but
     * instead are functions that know how to write the data.
     */
    private List<DataOutput> bufferFileFooter()
            throws IOException
    {
        List<DataOutput> outputData = new ArrayList<>();

        Metadata metadata = new Metadata(closedStripes.stream()
                .map(ClosedStripe::getStatistics)
                .collect(toList()));
        Slice metadataSlice = metadataWriter.writeMetadata(metadata);
        outputData.add(createDataOutput(metadataSlice));

        long numberOfRows = closedStripes.stream()
                .mapToLong(stripe -> stripe.getStripeInformation().getNumberOfRows())
                .sum();

        List<ColumnStatistics> fileStats = toFileStats(
                closedStripes.stream()
                        .map(ClosedStripe::getStatistics)
                        .map(StripeStatistics::getColumnStatistics)
                        .collect(toList()));
        recordValidation(validation -> validation.setFileStatistics(fileStats));

        Map<String, Slice> userMetadata = this.userMetadata.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> utf8Slice(entry.getValue())));

        List<ColumnStatistics> unencryptedStats = new ArrayList<>();
        Map<Integer, Map<Integer, Slice>> encryptedStats = new HashMap<>();
        addStatsRecursive(fileStats, 0, new HashMap<>(), unencryptedStats, encryptedStats);
        Optional<DwrfEncryption> dwrfEncryption;
        if (dwrfWriterEncryption.isPresent()) {
            ImmutableList.Builder<EncryptionGroup> encryptionGroupBuilder = ImmutableList.builder();
            List<WriterEncryptionGroup> writerEncryptionGroups = dwrfWriterEncryption.get().getWriterEncryptionGroups();
            for (int i = 0; i < writerEncryptionGroups.size(); i++) {
                WriterEncryptionGroup group = writerEncryptionGroups.get(i);
                Map<Integer, Slice> groupStats = encryptedStats.get(i);
                encryptionGroupBuilder.add(
                        new EncryptionGroup(
                                group.getNodes(),
                                Optional.empty(), // reader will just use key metadata from the stripe
                                group.getNodes().stream()
                                        .map(groupStats::get)
                                        .collect(toList())));
            }
            dwrfEncryption = Optional.of(
                    new DwrfEncryption(
                            dwrfWriterEncryption.get().getKeyProvider(),
                            encryptionGroupBuilder.build()));
        }
        else {
            dwrfEncryption = Optional.empty();
        }

        Footer footer = new Footer(
                numberOfRows,
                rowGroupMaxRowCount,
                closedStripes.stream()
                        .map(ClosedStripe::getStripeInformation)
                        .collect(toList()),
                orcTypes,
                ImmutableList.copyOf(unencryptedStats),
                userMetadata,
                dwrfEncryption);

        closedStripes.clear();
        closedStripesRetainedBytes = 0;

        Slice footerSlice = metadataWriter.writeFooter(footer);
        outputData.add(createDataOutput(footerSlice));

        recordValidation(validation -> validation.setVersion(metadataWriter.getOrcMetadataVersion()));
        Slice postscriptSlice = metadataWriter.writePostscript(footerSlice.length(), metadataSlice.length(), compressionParameters.getKind(), compressionParameters.getMaxBufferSize());
        outputData.add(createDataOutput(postscriptSlice));
        outputData.add(createDataOutput(Slices.wrappedBuffer((byte) postscriptSlice.length())));
        return outputData;
    }

    private void addStatsRecursive(List<ColumnStatistics> allStats, int index, Map<Integer, List<ColumnStatistics>> nodeAndSubNodeStats, List<ColumnStatistics> unencryptedStats, Map<Integer, Map<Integer, Slice>> encryptedStats)
            throws IOException
    {
        if (allStats.isEmpty()) {
            return;
        }
        ColumnStatistics columnStatistics = allStats.get(index);
        if (dwrfEncryptionInfo.getGroupByNodeId(index).isPresent()) {
            int group = dwrfEncryptionInfo.getGroupByNodeId(index).get();
            boolean isRootNode = dwrfWriterEncryption.get().getWriterEncryptionGroups().get(group).getNodes().contains(index);
            verify(isRootNode && nodeAndSubNodeStats.isEmpty() || nodeAndSubNodeStats.size() == 1 && nodeAndSubNodeStats.get(group) != null,
                    "nodeAndSubNodeStats should only be present for subnodes of a group");
            nodeAndSubNodeStats.computeIfAbsent(group, x -> new ArrayList<>()).add(columnStatistics);
            unencryptedStats.add(new ColumnStatistics(
                    columnStatistics.getNumberOfValues(),
                    columnStatistics.getMinAverageValueSizeInBytes(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null));
            for (Integer fieldIndex : orcTypes.get(index).getFieldTypeIndexes()) {
                addStatsRecursive(allStats, fieldIndex, nodeAndSubNodeStats, unencryptedStats, encryptedStats);
            }
            if (isRootNode) {
                Slice encryptedFileStatistics = toEncryptedFileStatistics(nodeAndSubNodeStats.get(group), group);
                encryptedStats.computeIfAbsent(group, x -> new HashMap<>()).put(index, encryptedFileStatistics);
            }
        }
        else {
            unencryptedStats.add(columnStatistics);
            for (Integer fieldIndex : orcTypes.get(index).getFieldTypeIndexes()) {
                addStatsRecursive(allStats, fieldIndex, new HashMap<>(), unencryptedStats, encryptedStats);
            }
        }
    }

    private Slice toEncryptedFileStatistics(List<ColumnStatistics> statsFromRoot, int groupId)
            throws IOException
    {
        DwrfProto.FileStatistics fileStatistics = toFileStatistics(statsFromRoot);
        DwrfDataEncryptor dwrfDataEncryptor = dwrfEncryptionInfo.getEncryptorByGroupId(groupId);
        OrcOutputBuffer buffer = new OrcOutputBuffer(compressionParameters, Optional.of(dwrfDataEncryptor));
        fileStatistics.writeTo(buffer);
        buffer.close();
        DynamicSliceOutput output = new DynamicSliceOutput(toIntExact(buffer.getOutputDataSize()));
        buffer.writeDataTo(output);
        return output.slice();
    }

    private void recordValidation(Consumer<OrcWriteValidationBuilder> task)
    {
        if (validationBuilder != null) {
            task.accept(validationBuilder);
        }
    }

    public void validate(OrcDataSource input)
            throws OrcCorruptionException
    {
        checkState(validationBuilder != null, "validation is not enabled");
        ImmutableMap.Builder<Integer, Slice> intermediateKeyMetadata = ImmutableMap.builder();
        if (dwrfWriterEncryption.isPresent()) {
            List<WriterEncryptionGroup> writerEncryptionGroups = dwrfWriterEncryption.get().getWriterEncryptionGroups();
            for (int i = 0; i < writerEncryptionGroups.size(); i++) {
                for (Integer node : writerEncryptionGroups.get(i).getNodes()) {
                    intermediateKeyMetadata.put(node, writerEncryptionGroups.get(i).getIntermediateKeyMetadata());
                }
            }
        }

        validateFile(
                validationBuilder.build(),
                input,
                types,
                hiveStorageTimeZone,
                orcEncoding,
                new OrcReaderOptions(new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE), new DataSize(16, MEGABYTE), false),
                dwrfEncryptionProvider,
                DwrfKeyProvider.of(intermediateKeyMetadata.build()));
    }

    private static <T> List<T> toDenseList(Map<Integer, T> data, int expectedSize)
    {
        checkArgument(data.size() == expectedSize);
        if (expectedSize == 0) {
            return ImmutableList.of();
        }
        ArrayList<T> list = new ArrayList<>(expectedSize);
        TreeSet<Integer> sortedKeys = new TreeSet<>();
        sortedKeys.addAll(data.keySet());
        for (Integer key : sortedKeys) {
            list.add(data.get(key));
        }
        return ImmutableList.copyOf(list);
    }

    private static List<ColumnStatistics> toFileStats(List<List<ColumnStatistics>> stripes)
    {
        if (stripes.isEmpty()) {
            return ImmutableList.of();
        }
        int columnCount = stripes.get(0).size();
        checkArgument(stripes.stream().allMatch(stripe -> columnCount == stripe.size()));

        ImmutableList.Builder<ColumnStatistics> fileStats = ImmutableList.builder();
        for (int i = 0; i < columnCount; i++) {
            int column = i;
            fileStats.add(ColumnStatistics.mergeColumnStatistics(stripes.stream()
                    .map(stripe -> stripe.get(column))
                    .collect(toList())));
        }
        return fileStats.build();
    }

    private static class ClosedStripe
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(ClosedStripe.class).instanceSize() + ClassLayout.parseClass(StripeInformation.class).instanceSize();

        private final StripeInformation stripeInformation;
        private final StripeStatistics statistics;

        public ClosedStripe(StripeInformation stripeInformation, StripeStatistics statistics)
        {
            this.stripeInformation = requireNonNull(stripeInformation, "stripeInformation is null");
            this.statistics = requireNonNull(statistics, "stripeStatistics is null");
        }

        public StripeInformation getStripeInformation()
        {
            return stripeInformation;
        }

        public StripeStatistics getStatistics()
        {
            return statistics;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + statistics.getRetainedSizeInBytes();
        }
    }
}
