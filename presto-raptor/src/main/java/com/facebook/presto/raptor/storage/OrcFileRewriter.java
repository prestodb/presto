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
package com.facebook.presto.raptor.storage;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.WriterStats;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.raptor.RaptorOrcAggregatedMemoryContext;
import com.facebook.presto.raptor.util.Closer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcPredicate.TRUE;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.HASHED;
import static com.facebook.presto.raptor.storage.OrcFileWriter.DEFAULT_OPTION;
import static com.facebook.presto.raptor.storage.OrcStorageManager.DEFAULT_STORAGE_TIMEZONE;
import static com.facebook.presto.raptor.storage.OrcStorageManager.HUGE_MAX_READ_BLOCK_SIZE;
import static com.facebook.presto.raptor.util.Closer.closer;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class OrcFileRewriter
{
    private static final Logger log = Logger.get(OrcFileRewriter.class);
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    private final ReaderAttributes readerAttributes;
    private final boolean validate;
    private final WriterStats stats;
    private final TypeManager typeManager;
    private final CompressionKind compression;
    private final OrcDataEnvironment orcDataEnvironment;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSource stripeMetadataSource;

    OrcFileRewriter(
            ReaderAttributes readerAttributes,
            boolean validate,
            WriterStats stats,
            TypeManager typeManager,
            OrcDataEnvironment orcDataEnvironment,
            CompressionKind compression,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource)
    {
        this.readerAttributes = requireNonNull(readerAttributes, "readerAttributes is null");
        this.validate = validate;
        this.stats = requireNonNull(stats, "stats is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.orcDataEnvironment = requireNonNull(orcDataEnvironment, "orcDataEnvironment is null");
        this.compression = requireNonNull(compression, "compression is null");
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSource = requireNonNull(stripeMetadataSource, "stripeMetadataSource is null");
    }

    public OrcFileInfo rewrite(FileSystem fileSystem, Map<String, Type> allColumnTypes, Path input, Path output, BitSet rowsToDelete)
            throws IOException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader());
                OrcDataSource dataSource = orcDataEnvironment.createOrcDataSource(fileSystem, input, readerAttributes)) {
            OrcReader reader = new OrcReader(
                    dataSource,
                    ORC,
                    orcFileTailSource,
                    stripeMetadataSource,
                    new RaptorOrcAggregatedMemoryContext(),
                    new OrcReaderOptions(readerAttributes.getMaxMergeDistance(), readerAttributes.getTinyStripeThreshold(), HUGE_MAX_READ_BLOCK_SIZE, readerAttributes.isZstdJniDecompressionEnabled()),
                    false,
                    NO_ENCRYPTION,
                    DwrfKeyProvider.EMPTY);

            if (reader.getFooter().getNumberOfRows() < rowsToDelete.length()) {
                throw new IOException("File has fewer rows than deletion vector");
            }
            int deleteRowCount = rowsToDelete.cardinality();
            if (reader.getFooter().getNumberOfRows() == deleteRowCount) {
                return new OrcFileInfo(0, 0);
            }
            if (reader.getFooter().getNumberOfRows() >= Integer.MAX_VALUE) {
                throw new IOException("File has too many rows");
            }
            int inputRowCount = toIntExact(reader.getFooter().getNumberOfRows());

            Map<String, Integer> currentColumnIds = IntStream.range(0, reader.getColumnNames().size()).boxed().collect(toMap(reader.getColumnNames()::get, i -> i));

            ImmutableList.Builder<Type> writerColumnTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<String> writerColumnIdsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Integer> readerColumnIndexBuilder = ImmutableList.builder();

            // Build columns for writer; keep the right ordinal
            Map<String, Type> orderedAllColumnTypes = new TreeMap<>(Comparator.comparingLong(Long::parseLong));
            orderedAllColumnTypes.putAll(allColumnTypes);
            for (Map.Entry<String, Type> columnType : orderedAllColumnTypes.entrySet()) {
                // Get the intersection of the provide columns and the actual columns
                Integer currentColumnIndex = currentColumnIds.get(columnType.getKey());
                if (currentColumnIndex != null) {
                    readerColumnIndexBuilder.add(currentColumnIndex);
                    writerColumnTypesBuilder.add(columnType.getValue());
                    writerColumnIdsBuilder.add(columnType.getKey());
                }
            }

            List<Type> writerColumnTypes = writerColumnTypesBuilder.build();
            List<String> writerColumnIds = writerColumnIdsBuilder.build();
            List<Integer> readerColumnIndex = readerColumnIndexBuilder.build();
            Map<Integer, Type> readerColumns = IntStream.range(0, readerColumnIndex.size()).boxed().collect(toMap(readerColumnIndex::get, writerColumnTypes::get));

            if (writerColumnTypes.isEmpty()) {
                // no intersection; directly return
                return new OrcFileInfo(0, 0);
            }

            StorageTypeConverter converter = new StorageTypeConverter(typeManager);
            List<Type> writerStorageTypes = writerColumnTypes.stream()
                    .map(converter::toStorageType)
                    .collect(toImmutableList());

            long start = System.nanoTime();

            Map<String, String> userMetadata = ImmutableMap.of();
            if (reader.getFooter().getUserMetadata().containsKey(OrcFileMetadata.KEY)) {
                // build metadata if the original file has it
                ImmutableMap.Builder<Long, TypeSignature> metadataBuilder = ImmutableMap.builder();
                for (int i = 0; i < writerColumnIds.size(); i++) {
                    metadataBuilder.put(Long.parseLong(writerColumnIds.get(i)), writerColumnTypes.get(i).getTypeSignature());
                }
                userMetadata = ImmutableMap.of(OrcFileMetadata.KEY, METADATA_CODEC.toJson(new OrcFileMetadata(metadataBuilder.build())));
            }

            StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);

            try (Closer<OrcBatchRecordReader, IOException> recordReader = closer(
                    reader.createBatchRecordReader(
                            storageTypeConverter.toStorageTypes(readerColumns),
                            TRUE,
                            DEFAULT_STORAGE_TIMEZONE,
                            new RaptorOrcAggregatedMemoryContext(),
                            INITIAL_BATCH_SIZE),
                    OrcBatchRecordReader::close);
                    Closer<OrcWriter, IOException> writer = closer(new OrcWriter(
                                    orcDataEnvironment.createOrcDataSink(fileSystem, output),
                                    writerColumnIds,
                                    writerStorageTypes,
                                    ORC,
                                    compression,
                                    Optional.empty(),
                                    NO_ENCRYPTION,
                                    DEFAULT_OPTION,
                                    userMetadata,
                                    DEFAULT_STORAGE_TIMEZONE,
                                    validate,
                                    HASHED,
                                    stats),
                            OrcWriter::close)) {
                OrcFileInfo fileInfo = rewrite(recordReader.get(), writer.get(), rowsToDelete, writerColumnTypes, readerColumnIndexBuilder.build());
                log.debug("Rewrote file %s in %s (input rows: %s, output rows: %s)", input.getName(), nanosSince(start), inputRowCount, inputRowCount - deleteRowCount);
                return fileInfo;
            }
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
    }

    private static OrcFileInfo rewrite(
            OrcBatchRecordReader reader,
            OrcWriter writer,
            BitSet rowsToDelete,
            List<Type> types,
            List<Integer> readerColumnIndex)
            throws IOException
    {
        int row = rowsToDelete.nextClearBit(0);
        long rowCount = 0;
        long uncompressedSize = 0;

        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }

            if (row >= (reader.getFilePosition() + batchSize)) {
                continue;
            }

            Block[] blocks = new Block[types.size()];
            for (int i = 0; i < types.size(); i++) {
                // read from existing columns
                blocks[i] = reader.readBlock(readerColumnIndex.get(i));
            }

            row = toIntExact(reader.getFilePosition());

            Page page = maskedPage(blocks, rowsToDelete, row, batchSize);
            writer.write(page);

            rowCount += page.getPositionCount();
            uncompressedSize += page.getLogicalSizeInBytes();

            row = rowsToDelete.nextClearBit(row + batchSize);
        }

        return new OrcFileInfo(rowCount, uncompressedSize);
    }

    private static Page maskedPage(Block[] blocks, BitSet rowsToDelete, int start, int count)
    {
        int[] ids = new int[count];
        int size = 0;
        for (int i = 0; i < count; i++) {
            if (!rowsToDelete.get(start + i)) {
                ids[size] = i;
                size++;
            }
        }

        return new Page(blocks).getPositions(ids, 0, size);
    }
}
