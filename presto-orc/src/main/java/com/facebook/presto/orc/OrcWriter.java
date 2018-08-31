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

import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationBuilder;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.orc.OrcWriterStats.FlushReason;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.Metadata;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.stream.OrcDataOutput;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.facebook.presto.orc.writer.ColumnWriter;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.presto.orc.OrcReader.validateFile;
import static com.facebook.presto.orc.OrcWriterStats.FlushReason.CLOSED;
import static com.facebook.presto.orc.OrcWriterStats.FlushReason.DICTIONARY_FULL;
import static com.facebook.presto.orc.OrcWriterStats.FlushReason.MAX_BYTES;
import static com.facebook.presto.orc.OrcWriterStats.FlushReason.MAX_ROWS;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.PostScript.MAGIC;
import static com.facebook.presto.orc.stream.OrcDataOutput.createDataOutput;
import static com.facebook.presto.orc.writer.ColumnWriters.createColumnWriter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Integer.min;
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
    private final OrcWriterStats stats;

    static {
        String version = OrcWriter.class.getPackage().getImplementationVersion();
        PRESTO_ORC_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final OrcDataSink orcDataSink;
    private final List<Type> types;
    private final OrcEncoding orcEncoding;
    private final CompressionKind compression;
    private final int stripeMinBytes;
    private final int stripeMaxBytes;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final int maxCompressionBufferSize;
    private final Map<String, String> userMetadata;
    private final CompressedMetadataWriter metadataWriter;
    private final DateTimeZone hiveStorageTimeZone;

    private final List<ClosedStripe> closedStripes = new ArrayList<>();
    private final List<OrcType> orcTypes;

    private final List<ColumnWriter> columnWriters;
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
            OrcDataSink orcDataSink,
            List<String> columnNames,
            List<Type> types,
            OrcEncoding orcEncoding,
            CompressionKind compression,
            OrcWriterOptions options,
            Map<String, String> userMetadata,
            DateTimeZone hiveStorageTimeZone,
            boolean validate,
            OrcWriteValidationMode validationMode,
            OrcWriterStats stats)
    {
        this.validationBuilder = validate ? new OrcWriteValidation.OrcWriteValidationBuilder(validationMode, types).setStringStatisticsLimitInBytes(toIntExact(options.getMaxStringStatisticsLimit().toBytes())) : null;

        this.orcDataSink = requireNonNull(orcDataSink, "orcDataSink is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.orcEncoding = requireNonNull(orcEncoding, "orcEncoding is null");
        this.compression = requireNonNull(compression, "compression is null");
        recordValidation(validation -> validation.setCompression(compression));

        requireNonNull(options, "options is null");
        checkArgument(options.getStripeMaxSize().compareTo(options.getStripeMinSize()) >= 0, "stripeMaxSize must be greater than stripeMinSize");
        this.stripeMinBytes = toIntExact(requireNonNull(options.getStripeMinSize(), "stripeMinSize is null").toBytes());
        this.stripeMaxBytes = toIntExact(requireNonNull(options.getStripeMaxSize(), "stripeMaxSize is null").toBytes());
        this.stripeMaxRowCount = options.getStripeMaxRowCount();
        this.rowGroupMaxRowCount = options.getRowGroupMaxRowCount();
        recordValidation(validation -> validation.setRowGroupMaxRowCount(rowGroupMaxRowCount));
        this.maxCompressionBufferSize = toIntExact(options.getMaxCompressionBufferSize().toBytes());

        this.userMetadata = ImmutableMap.<String, String>builder()
                .putAll(requireNonNull(userMetadata, "userMetadata is null"))
                .put(PRESTO_ORC_WRITER_VERSION_METADATA_KEY, PRESTO_ORC_WRITER_VERSION)
                .build();
        this.metadataWriter = new CompressedMetadataWriter(orcEncoding.createMetadataWriter(), compression, maxCompressionBufferSize);
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.stats = requireNonNull(stats, "stats is null");

        requireNonNull(columnNames, "columnNames is null");
        this.orcTypes = OrcType.createOrcRowType(0, columnNames, types);
        recordValidation(validation -> validation.setColumnNames(columnNames));

        // create column writers
        OrcType rootType = orcTypes.get(0);
        checkArgument(rootType.getFieldCount() == types.size());
        ImmutableList.Builder<ColumnWriter> columnWriters = ImmutableList.builder();
        ImmutableSet.Builder<SliceDictionaryColumnWriter> sliceColumnWriters = ImmutableSet.builder();
        for (int fieldId = 0; fieldId < types.size(); fieldId++) {
            int fieldColumnIndex = rootType.getFieldTypeIndex(fieldId);
            Type fieldType = types.get(fieldId);
            ColumnWriter columnWriter = createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, maxCompressionBufferSize, orcEncoding, hiveStorageTimeZone, options.getMaxStringStatisticsLimit());
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
        this.dictionaryCompressionOptimizer = new DictionaryCompressionOptimizer(
                sliceColumnWriters.build(),
                stripeMinBytes,
                stripeMaxBytes,
                stripeMaxRowCount,
                toIntExact(requireNonNull(options.getDictionaryMaxMemory(), "dictionaryMaxMemory is null").toBytes()));

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
        return orcDataSink.size();
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
                orcDataSink.getRetainedSizeInBytes() +
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

        while (page != null) {
            // align page to row group boundaries
            Page chunk;
            if (rowGroupRowCount + page.getPositionCount() > rowGroupMaxRowCount || stripeRowCount + page.getPositionCount() > stripeMaxRowCount) {
                int chunkRows = min(rowGroupMaxRowCount - rowGroupRowCount, stripeMaxRowCount - stripeRowCount);
                chunk = page.getRegion(0, chunkRows);
                page = page.getRegion(chunkRows, page.getPositionCount() - chunkRows);
            }
            else {
                chunk = page;
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
        List<OrcDataOutput> outputData = new ArrayList<>();
        long stripeStartOffset = orcDataSink.size();
        // add header to first stripe (this is not required but nice to have)
        if (closedStripes.isEmpty()) {
            outputData.add(createDataOutput(MAGIC));
            stripeStartOffset += MAGIC.length();
        }
        // add stripe data
        outputData.addAll(bufferStripeData(stripeStartOffset, flushReason));
        // if the file is being closed, add the file footer
        if (flushReason == CLOSED) {
            outputData.addAll(bufferFileFooter());
        }

        // write all data
        orcDataSink.write(outputData);

        // open next stripe
        columnWriters.forEach(ColumnWriter::reset);
        dictionaryCompressionOptimizer.reset();
        rowGroupRowCount = 0;
        stripeRowCount = 0;
        bufferedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum());
    }

    /**
     * Collect the data for for the stripe.  This is not the actual data, but
     * instead are functions that know how to write the data.
     */
    private List<OrcDataOutput> bufferStripeData(long stripeStartOffset, FlushReason flushReason)
            throws IOException
    {
        if (stripeRowCount == 0) {
            verify(flushReason == CLOSED, "An empty stripe is not allowed");
            // column writers must be closed or the reset call will fail
            columnWriters.forEach(ColumnWriter::close);
            return ImmutableList.of();
        }

        if (rowGroupRowCount > 0) {
            finishRowGroup();
        }

        // convert any dictionary encoded column with a low compression ratio to direct
        dictionaryCompressionOptimizer.finalOptimize(bufferedBytes);

        columnWriters.forEach(ColumnWriter::close);

        List<OrcDataOutput> outputData = new ArrayList<>();
        List<Stream> allStreams = new ArrayList<>(columnWriters.size() * 3);

        // get index streams
        long indexLength = 0;
        for (ColumnWriter columnWriter : columnWriters) {
            for (StreamDataOutput indexStream : columnWriter.getIndexStreams(metadataWriter)) {
                // The ordering is critical because the stream only contain a length with no offset.
                outputData.add(indexStream);
                allStreams.add(indexStream.getStream());
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
            // The ordering is critical because the stream only contain a length with no offset.
            outputData.add(dataStream);
            allStreams.add(dataStream.getStream());
        }

        Map<Integer, ColumnEncoding> columnEncodings = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnEncodings.putAll(columnWriter.getColumnEncodings()));

        Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.getColumnStripeStatistics()));

        // the 0th column is a struct column for the whole row
        columnEncodings.put(0, new ColumnEncoding(DIRECT, 0));
        columnStatistics.put(0, new ColumnStatistics((long) stripeRowCount, 0, null, null, null, null, null, null, null, null));

        // add footer
        StripeFooter stripeFooter = new StripeFooter(allStreams, toDenseList(columnEncodings, orcTypes.size()));
        Slice footer = metadataWriter.writeStripeFooter(stripeFooter);
        outputData.add(createDataOutput(footer));

        // create final stripe statistics
        StripeStatistics statistics = new StripeStatistics(toDenseList(columnStatistics, orcTypes.size()));
        recordValidation(validation -> validation.addStripeStatistics(stripeStartOffset, statistics));
        StripeInformation stripeInformation = new StripeInformation(stripeRowCount, stripeStartOffset, indexLength, dataLength, footer.length());
        ClosedStripe closedStripe = new ClosedStripe(stripeInformation, statistics);
        closedStripes.add(closedStripe);
        closedStripesRetainedBytes += closedStripe.getRetainedSizeInBytes();
        recordValidation(validation -> validation.addStripe(stripeInformation.getNumberOfRows()));
        stats.recordStripeWritten(flushReason, stripeInformation.getTotalLength(), stripeInformation.getNumberOfRows(), dictionaryCompressionOptimizer.getDictionaryMemoryBytes());

        if (flushReason == MAX_BYTES && stripeInformation.getTotalLength() < stripeMaxBytes / 2
                || flushReason == DICTIONARY_FULL && stripeInformation.getTotalLength() < stripeMinBytes / 2) {
            log.debug("Small stripe with size %s get %s flush. QueryId: %s", stripeInformation.getTotalLength(), flushReason, userMetadata.get("presto_query_id"));
        }

        return outputData;
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

        orcDataSink.close();
    }

    /**
     * Collect the data for for the file footer.  This is not the actual data, but
     * instead are functions that know how to write the data.
     */
    private List<OrcDataOutput> bufferFileFooter()
            throws IOException
    {
        List<OrcDataOutput> outputData = new ArrayList<>();

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

        Footer footer = new Footer(
                numberOfRows,
                rowGroupMaxRowCount,
                closedStripes.stream()
                        .map(ClosedStripe::getStripeInformation)
                        .collect(toList()),
                orcTypes,
                fileStats,
                userMetadata);

        closedStripes.clear();
        closedStripesRetainedBytes = 0;

        Slice footerSlice = metadataWriter.writeFooter(footer);
        outputData.add(createDataOutput(footerSlice));

        recordValidation(validation -> validation.setVersion(metadataWriter.getOrcMetadataVersion()));
        Slice postscriptSlice = metadataWriter.writePostscript(footerSlice.length(), metadataSlice.length(), compression, maxCompressionBufferSize);
        outputData.add(createDataOutput(postscriptSlice));
        outputData.add(createDataOutput(Slices.wrappedBuffer((byte) postscriptSlice.length())));
        return outputData;
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

        validateFile(
                validationBuilder.build(),
                input,
                types,
                hiveStorageTimeZone,
                orcEncoding);
    }

    private static <T> List<T> toDenseList(Map<Integer, T> data, int expectedSize)
    {
        checkArgument(data.size() == expectedSize);
        ArrayList<T> list = new ArrayList<>(expectedSize);
        for (int i = 0; i < expectedSize; i++) {
            list.add(data.get(i));
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
