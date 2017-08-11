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
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.DwrfMetadataWriter;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.Metadata;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcMetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.writer.ColumnWriter;
import com.facebook.presto.orc.writer.ColumnWriters;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.presto.orc.OrcReader.validateFile;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.PostScript.MAGIC;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Integer.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcWriter
        implements Closeable
{
    private static final int COMPRESSION_BLOCK_SIZE = 262_144;
    public static final DataSize DEFAULT_STRIPE_MAX_SIZE = new DataSize(256, MEGABYTE);
    public static final int DEFAULT_STRIPE_MIN_ROW_COUNT = 100_000;
    public static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;
    public static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    public static final int DEFAULT_BUFFER_SIZE = 256 * 1024;
    public static final DataSize DEFAULT_DICTIONARY_MEMORY_MAX_SIZE = new DataSize(32, MEGABYTE);

    static final String PRESTO_ORC_WRITER_VERSION_METADATA_KEY = "presto.writer.version";
    static final String PRESTO_ORC_WRITER_VERSION;
    static {
        String version = OrcWriter.class.getPackage().getImplementationVersion();
        PRESTO_ORC_WRITER_VERSION = version == null ? "UNKNOWN" : version;
    }

    private final SliceOutput output;
    private final List<Type> types;
    private final CompressionKind compression;
    private final int stripeMaxBytes;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final Map<String, String> userMetadata;
    private final MetadataWriter metadataWriter;
    private final DateTimeZone hiveStorageTimeZone;

    private final List<ClosedStripe> closedStripes = new ArrayList<>();
    private final List<OrcType> orcTypes;

    private final List<ColumnWriter> columnWriters;
    private final DictionaryCompressionOptimizer dictionaryCompressionOptimizer;
    private long stripeStartOffset;
    private int stripeRowCount;
    private int rowGroupRowCount;
    private int bufferedBytes;
    private int retainedBytes;
    private boolean closed;

    @Nullable
    private OrcWriteValidation.OrcWriteValidationBuilder validationBuilder;

    public static OrcWriter createOrcWriter(
            SliceOutput output,
            List<String> columnNames,
            List<Type> types,
            CompressionKind compression,
            DataSize stripeMaxBytes,
            int stripeMinRowCount,
            int stripeMaxRowCount,
            int rowGroupMaxRowCount,
            DataSize dictionaryMemoryMaxBytes,
            Map<String, String> userMetadata,
            DateTimeZone hiveStorageTimeZone,
            boolean validate)
    {
        return new OrcWriter(
                output,
                columnNames,
                types,
                compression,
                stripeMaxBytes,
                stripeMinRowCount,
                stripeMaxRowCount,
                rowGroupMaxRowCount,
                dictionaryMemoryMaxBytes,
                userMetadata,
                new OrcMetadataWriter(),
                false,
                hiveStorageTimeZone,
                validate);
    }

    public static OrcWriter createDwrfWriter(
            SliceOutput output,
            List<String> columnNames,
            List<Type> types,
            CompressionKind compression,
            DataSize stripeMaxBytes,
            int stripeMinRowCount,
            int stripeMaxRowCount,
            int rowGroupMaxRowCount,
            DataSize dictionaryMemoryMaxBytes,
            Map<String, String> userMetadata,
            DateTimeZone hiveStorageTimeZone,
            boolean validate)
    {
        return new OrcWriter(
                output,
                columnNames,
                types,
                compression,
                stripeMaxBytes,
                stripeMinRowCount,
                stripeMaxRowCount,
                rowGroupMaxRowCount,
                dictionaryMemoryMaxBytes,
                userMetadata,
                new DwrfMetadataWriter(),
                true,
                hiveStorageTimeZone,
                validate);
    }

    private OrcWriter(
            SliceOutput output,
            List<String> columnNames,
            List<Type> types,
            CompressionKind compression,
            DataSize stripeMaxBytes,
            int stripeMinRowCount,
            int stripeMaxRowCount,
            int rowGroupMaxRowCount,
            DataSize dictionaryMemoryMaxBytes,
            Map<String, String> userMetadata,
            MetadataWriter metadataWriter,
            boolean isDwrf,
            DateTimeZone hiveStorageTimeZone,
            boolean validate)
    {
        this.validationBuilder = validate ? new OrcWriteValidation.OrcWriteValidationBuilder(types) : null;

        this.output = requireNonNull(output, "output is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.compression = requireNonNull(compression, "compression is null");
        recordValidation(validation -> validation.setCompression(compression));
        this.stripeMaxBytes = toIntExact(requireNonNull(stripeMaxBytes, "stripeMaxSize is null").toBytes());
        checkArgument(stripeMinRowCount >= 1, "stripeMinRowCount must be at least 1");
        checkArgument(stripeMaxRowCount >= stripeMinRowCount, "stripeMaxRowCount must be greater than stripeMinRowCount");
        this.stripeMaxRowCount = stripeMaxRowCount;
        checkArgument(rowGroupMaxRowCount >= 1, "rowGroupMaxRowCount must be at least 1");
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        recordValidation(validation -> validation.setRowGroupMaxRowCount(rowGroupMaxRowCount));
        this.userMetadata = ImmutableMap.<String, String>builder()
                .putAll(requireNonNull(userMetadata, "userMetadata is null"))
                .put(PRESTO_ORC_WRITER_VERSION_METADATA_KEY, PRESTO_ORC_WRITER_VERSION)
                .build();
        this.metadataWriter = new CompressedMetadataWriter(
                requireNonNull(metadataWriter, "metadataWriter is null"),
                compression,
                DEFAULT_BUFFER_SIZE);
        this.hiveStorageTimeZone = requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");

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
            ColumnWriter columnWriter = ColumnWriters.createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, DEFAULT_BUFFER_SIZE, isDwrf, hiveStorageTimeZone);
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
                toIntExact(stripeMaxBytes.toBytes()),
                stripeMinRowCount,
                stripeMaxRowCount,
                toIntExact(requireNonNull(dictionaryMemoryMaxBytes, "dictionaryMemoryMaxSize is null").toBytes()));

        // this is not required but nice to have
        output.writeBytes(MAGIC);
        stripeStartOffset = output.size();

        for (Entry<String, String> entry : this.userMetadata.entrySet()) {
            recordValidation(validation -> validation.addMetadataProperty(entry.getKey(), utf8Slice(entry.getValue())));
        }
    }

    public int getBufferedBytes()
    {
        return bufferedBytes;
    }

    public long getRetainedBytes()
    {
        return retainedBytes;
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
        if (stripeRowCount == stripeMaxRowCount || bufferedBytes > stripeMaxBytes || dictionaryCompressionOptimizer.isFull()) {
            writeStripe();
        }

        retainedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum());
    }

    private void finishRowGroup()
    {
        Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.finishRowGroup()));
        recordValidation(validation -> validation.addRowGroupStatistics(columnStatistics));
        rowGroupRowCount = 0;
    }

    private void writeStripe()
            throws IOException
    {
        if (stripeRowCount == 0) {
            return;
        }

        recordValidation(validation -> validation.addStripe(stripeRowCount));

        if (rowGroupRowCount > 0) {
            finishRowGroup();
        }

        // convert any dictionary encoded column with a low compression ratio to direct
        dictionaryCompressionOptimizer.finalOptimize();

        columnWriters.forEach(ColumnWriter::close);

        List<Stream> allStreams = new ArrayList<>();

        // write index streams
        long indexLength = 0;
        for (ColumnWriter columnWriter : columnWriters) {
            List<Stream> indexStreams = columnWriter.writeIndexStreams(output, metadataWriter);
            allStreams.addAll(indexStreams);
            indexLength += indexStreams.stream()
                    .mapToInt(Stream::getLength)
                    .asLongStream()
                    .sum();
        }

        // write data streams
        long dataLength = 0;
        for (ColumnWriter columnWriter : columnWriters) {
            List<Stream> dataStreams = columnWriter.writeDataStreams(output);
            allStreams.addAll(dataStreams);
            dataLength += dataStreams.stream()
                    .mapToInt(Stream::getLength)
                    .asLongStream()
                    .sum();
        }

        Map<Integer, ColumnEncoding> columnEncodings = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnEncodings.putAll(columnWriter.getColumnEncodings()));

        Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();
        columnWriters.forEach(columnWriter -> columnStatistics.putAll(columnWriter.getColumnStripeStatistics()));

        // the 0th column is a struct column for the whole row
        columnEncodings.put(0, new ColumnEncoding(DIRECT, 0));
        columnStatistics.put(0, new ColumnStatistics((long) stripeRowCount, 0, null, null, null, null, null, null, null, null));

        StripeFooter stripeFooter = new StripeFooter(allStreams, toDenseList(columnEncodings, orcTypes.size()));
        int footerLength = metadataWriter.writeStripeFooter(output, stripeFooter);

        StripeStatistics statistics = new StripeStatistics(toDenseList(columnStatistics, orcTypes.size()));
        recordValidation(validation -> validation.addStripeStatistics(stripeStartOffset, statistics));
        StripeInformation stripeInformation = new StripeInformation(stripeRowCount, stripeStartOffset, indexLength, dataLength, footerLength);
        closedStripes.add(new ClosedStripe(stripeInformation, statistics));

        // open next stripe
        columnWriters.forEach(ColumnWriter::reset);
        dictionaryCompressionOptimizer.reset();
        rowGroupRowCount = 0;
        stripeRowCount = 0;
        stripeStartOffset = output.size();
        bufferedBytes = toIntExact(columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum());
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        writeStripe();

        Metadata metadata = new Metadata(closedStripes.stream()
                .map(ClosedStripe::getStatistics)
                .collect(toList()));
        int metadataLength = metadataWriter.writeMetadata(output, metadata);

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

        int footerLength = metadataWriter.writeFooter(output, footer);

        recordValidation(validation -> validation.setVersion(metadataWriter.getOrcMetadataVersion()));
        int postScriptLength = metadataWriter.writePostscript(output, footerLength, metadataLength, compression, COMPRESSION_BLOCK_SIZE);

        output.writeByte(postScriptLength);

        output.close();
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
                metadataWriter.getMetadataReader());
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
    }
}
