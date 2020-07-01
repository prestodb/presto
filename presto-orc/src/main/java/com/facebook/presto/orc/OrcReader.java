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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.ExceptionWrappingMetadataReader;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.Metadata;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.orc.DwrfEncryptionInfo.createNodeToGroupMap;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcReader
{
    public static final int MAX_BATCH_SIZE = 1024;
    public static final int INITIAL_BATCH_SIZE = 1;
    public static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    private final OrcDataSource orcDataSource;
    private final ExceptionWrappingMetadataReader metadataReader;
    private final HiveWriterVersion hiveWriterVersion;
    private final int bufferSize;
    private final CompressionKind compressionKind;
    private final Optional<OrcDecompressor> decompressor;
    private final Optional<EncryptionLibrary> encryptionLibrary;
    private final Map<Integer, Integer> dwrfEncryptionGroupMap;
    private final Footer footer;
    private final Metadata metadata;

    private final Optional<OrcWriteValidation> writeValidation;

    private final StripeMetadataSource stripeMetadataSource;
    private final OrcReaderOptions orcReaderOptions;

    private final boolean cacheable;

    // This is based on the Apache Hive ORC code
    public OrcReader(
            OrcDataSource orcDataSource,
            OrcEncoding orcEncoding,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource,
            OrcAggregatedMemoryContext aggregatedMemoryContext,
            OrcReaderOptions orcReaderOptions,
            boolean cacheable,
            DwrfEncryptionProvider dwrfEncryptionProvider)
            throws IOException
    {
        this(
                orcDataSource,
                orcEncoding,
                orcFileTailSource,
                stripeMetadataSource,
                Optional.empty(),
                aggregatedMemoryContext,
                orcReaderOptions,
                cacheable,
                dwrfEncryptionProvider);
    }

    OrcReader(
            OrcDataSource orcDataSource,
            OrcEncoding orcEncoding,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource,
            Optional<OrcWriteValidation> writeValidation,
            OrcAggregatedMemoryContext aggregatedMemoryContext,
            OrcReaderOptions orcReaderOptions,
            boolean cacheable,
            DwrfEncryptionProvider dwrfEncryptionProvider)
            throws IOException
    {
        this.orcReaderOptions = requireNonNull(orcReaderOptions, "orcReaderOptions is null");
        orcDataSource = wrapWithCacheIfTiny(orcDataSource, orcReaderOptions.getTinyStripeThreshold());
        this.orcDataSource = orcDataSource;
        requireNonNull(orcEncoding, "orcEncoding is null");
        this.metadataReader = new ExceptionWrappingMetadataReader(orcDataSource.getId(), orcEncoding.createMetadataReader());

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");

        this.stripeMetadataSource = requireNonNull(stripeMetadataSource, "stripeMetadataSource is null");

        OrcFileTail orcFileTail = orcFileTailSource.getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable);
        this.bufferSize = orcFileTail.getBufferSize();
        this.compressionKind = orcFileTail.getCompressionKind();
        this.decompressor = createOrcDecompressor(orcDataSource.getId(), compressionKind, bufferSize, orcReaderOptions.isOrcZstdJniDecompressionEnabled());
        this.hiveWriterVersion = orcFileTail.getHiveWriterVersion();

        try (InputStream footerInputStream = new OrcInputStream(orcDataSource.getId(), orcFileTail.getFooterSlice().getInput(), decompressor, Optional.empty(), aggregatedMemoryContext, orcFileTail.getFooterSize())) {
            this.footer = metadataReader.readFooter(hiveWriterVersion, footerInputStream);
        }
        if (this.footer.getTypes().size() == 0) {
            throw new OrcCorruptionException(orcDataSource.getId(), "File has no columns");
        }
        validateEncryption(this.footer, this.orcDataSource.getId());

        Optional<DwrfEncryption> encryption = footer.getEncryption();
        if (encryption.isPresent()) {
            this.dwrfEncryptionGroupMap = createNodeToGroupMap(
                    encryption.get().getEncryptionGroups().stream()
                            .map(EncryptionGroup::getNodes)
                            .collect(toImmutableList()),
                    footer.getTypes());
            this.encryptionLibrary = Optional.of(dwrfEncryptionProvider.getEncryptionLibrary(encryption.get().getKeyProvider()));
        }
        else {
            this.dwrfEncryptionGroupMap = ImmutableMap.of();
            this.encryptionLibrary = Optional.empty();
        }

        try (InputStream metadataInputStream = new OrcInputStream(orcDataSource.getId(), orcFileTail.getMetadataSlice().getInput(), decompressor, Optional.empty(), aggregatedMemoryContext, orcFileTail.getMetadataSize())) {
            this.metadata = metadataReader.readMetadata(hiveWriterVersion, metadataInputStream);
        }
        validateWrite(writeValidation, orcDataSource, validation -> validation.getColumnNames().equals(footer.getTypes().get(0).getFieldNames()), "Unexpected column names");
        validateWrite(writeValidation, orcDataSource, validation -> validation.getRowGroupMaxRowCount() == footer.getRowsInRowGroup(), "Unexpected rows in group");
        if (writeValidation.isPresent()) {
            writeValidation.get().validateMetadata(orcDataSource.getId(), footer.getUserMetadata());
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), footer.getFileStats());
            writeValidation.get().validateStripeStatistics(orcDataSource.getId(), footer.getStripes(), metadata.getStripeStatsList());
        }

        this.cacheable = requireNonNull(cacheable, "hiveFileContext is null");
    }

    @VisibleForTesting
    public static void validateEncryption(Footer footer, OrcDataSourceId dataSourceId)
    {
        if (!footer.getEncryption().isPresent()) {
            return;
        }
        footer.getEncryption().get();
        DwrfEncryption dwrfEncryption = footer.getEncryption().get();
        int encryptionGroupSize = dwrfEncryption.getEncryptionGroups().size();
        List<StripeInformation> stripes = footer.getStripes();
        if (!stripes.isEmpty() && encryptionGroupSize > 0 && stripes.get(0).getKeyMetadata().isEmpty()) {
            throw new OrcCorruptionException(dataSourceId, "Stripe encryption keys are missing, but file is encrypted");
        }
        for (StripeInformation stripe : stripes) {
            if (!stripe.getKeyMetadata().isEmpty() && stripe.getKeyMetadata().size() != encryptionGroupSize) {
                throw new OrcCorruptionException(
                        dataSourceId,
                        "Number of stripe encryption keys did not match number of encryption groups.  Expected %s, but found %s",
                        encryptionGroupSize,
                        stripe.getKeyMetadata().size());
            }
        }
    }

    public List<String> getColumnNames()
    {
        return footer.getTypes().get(0).getFieldNames();
    }

    public List<OrcType> getTypes()
    {
        return footer.getTypes();
    }

    public Footer getFooter()
    {
        return footer;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public OrcBatchRecordReader createBatchRecordReader(
            Map<Integer, Type> includedColumns,
            OrcPredicate predicate,
            DateTimeZone hiveStorageTimeZone,
            OrcAggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize,
            Map<Integer, Slice> columnsToIntermediateKeys)
            throws OrcCorruptionException
    {
        return createBatchRecordReader(includedColumns, predicate, 0, orcDataSource.getSize(), hiveStorageTimeZone, systemMemoryUsage, initialBatchSize, columnsToIntermediateKeys);
    }

    public OrcBatchRecordReader createBatchRecordReader(
            Map<Integer, Type> includedColumns,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone hiveStorageTimeZone,
            OrcAggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize,
            Map<Integer, Slice> columnsToIntermediateKeys)
            throws OrcCorruptionException
    {
        return new OrcBatchRecordReader(
                requireNonNull(includedColumns, "includedColumns is null"),
                requireNonNull(predicate, "predicate is null"),
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                decompressor,
                encryptionLibrary,
                dwrfEncryptionGroupMap,
                columnsToIntermediateKeys,
                footer.getRowsInRowGroup(),
                requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null"),
                hiveWriterVersion,
                metadataReader,
                orcReaderOptions.getMaxMergeDistance(),
                orcReaderOptions.getTinyStripeThreshold(),
                orcReaderOptions.getMaxBlockSize(),
                footer.getUserMetadata(),
                systemMemoryUsage.newOrcAggregatedMemoryContext(),
                writeValidation,
                initialBatchSize,
                stripeMetadataSource,
                cacheable);
    }

    public OrcSelectiveRecordReader createSelectiveRecordReader(
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            Map<Integer, Map<Subfield, TupleDomainFilter>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputs,
            Map<Integer, List<Subfield>> requiredSubfields,
            Map<Integer, Object> constantValues,
            Map<Integer, Function<Block, Block>> coercers,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone hiveStorageTimeZone,
            boolean legacyMapSubscript,
            OrcAggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            Map<Integer, Slice> columnsToIntermediateKeys)
    {
        return new OrcSelectiveRecordReader(
                includedColumns,
                outputColumns,
                filters,
                filterFunctions,
                filterFunctionInputs,
                requiredSubfields,
                constantValues,
                coercers,
                predicate,
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                decompressor,
                encryptionLibrary,
                dwrfEncryptionGroupMap,
                columnsToIntermediateKeys,
                footer.getRowsInRowGroup(),
                hiveStorageTimeZone,
                legacyMapSubscript,
                hiveWriterVersion,
                metadataReader,
                orcReaderOptions.getMaxMergeDistance(),
                orcReaderOptions.getTinyStripeThreshold(),
                orcReaderOptions.getMaxBlockSize(),
                footer.getUserMetadata(),
                systemMemoryUsage.newOrcAggregatedMemoryContext(),
                writeValidation,
                initialBatchSize,
                stripeMetadataSource,
                cacheable);
    }

    private static OrcDataSource wrapWithCacheIfTiny(OrcDataSource dataSource, DataSize maxCacheSize)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        if (dataSource.getSize() > maxCacheSize.toBytes()) {
            return dataSource;
        }
        DiskRange diskRange = new DiskRange(0, toIntExact(dataSource.getSize()));
        return new CachingOrcDataSource(dataSource, desiredOffset -> diskRange);
    }

    static void validateFile(
            OrcWriteValidation writeValidation,
            OrcDataSource input,
            List<Type> types,
            DateTimeZone hiveStorageTimeZone,
            OrcEncoding orcEncoding,
            OrcReaderOptions orcReaderOptions,
            Map<Integer, Slice> columnsToKeyMetadata,
            DwrfEncryptionProvider dwrfEncryptionProvider)
            throws OrcCorruptionException
    {
        ImmutableMap.Builder<Integer, Type> readTypes = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < types.size(); columnIndex++) {
            readTypes.put(columnIndex, types.get(columnIndex));
        }
        try {
            OrcReader orcReader = new OrcReader(
                    input,
                    orcEncoding,
                    new StorageOrcFileTailSource(),
                    new StorageStripeMetadataSource(),
                    Optional.of(writeValidation),
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    orcReaderOptions,
                    false,
                    dwrfEncryptionProvider);
            try (OrcBatchRecordReader orcRecordReader = orcReader.createBatchRecordReader(
                    readTypes.build(),
                    OrcPredicate.TRUE,
                    hiveStorageTimeZone,
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    INITIAL_BATCH_SIZE,
                    columnsToKeyMetadata)) {
                while (orcRecordReader.nextBatch() >= 0) {
                    // ignored
                }
            }
        }
        catch (IOException e) {
            throw new OrcCorruptionException(e, input.getId(), "Validation failed");
        }
    }

    public static void validateWrite(Optional<OrcWriteValidation> writeValidation, OrcDataSource orcDataSource, Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }
}
