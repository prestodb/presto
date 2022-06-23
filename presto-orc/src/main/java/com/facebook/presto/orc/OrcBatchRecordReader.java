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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.BatchStreamReader;
import com.facebook.presto.orc.reader.BatchStreamReaders;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OrcBatchRecordReader
        extends AbstractOrcRecordReader<BatchStreamReader>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcBatchRecordReader.class).instanceSize();

    public OrcBatchRecordReader(
            Map<Integer, Type> includedColumns,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            Optional<EncryptionLibrary> encryptionLibrary,
            Map<Integer, Integer> dwrfEncryptionGroupMap,
            Map<Integer, Slice> intermediateKeyMetadata,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            Map<String, Slice> userMetadata,
            OrcAggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            StripeMetadataSource stripeMetadataSource,
            boolean cacheable,
            RuntimeStats runtimeStats)
            throws OrcCorruptionException

    {
        super(includedColumns,
                ImmutableMap.of(),
                // The streamReadersSystemMemoryContext covers the StreamReader local buffer sizes, plus leaf node StreamReaders'
                // instance sizes who use local buffers. SliceDirectStreamReader's instance size is not counted, because it
                // doesn't have a local buffer. All non-leaf level StreamReaders' (e.g. MapStreamReader, LongStreamReader,
                // ListStreamReader and StructStreamReader) instance sizes were not counted, because calling setBytes() in
                // their constructors is confusing.
                createStreamReaders(orcDataSource, types, hiveStorageTimeZone, options, includedColumns, systemMemoryUsage.newOrcAggregatedMemoryContext()),
                predicate,
                numberOfRows,
                fileStripes,
                fileStats,
                stripeStats,
                orcDataSource,
                splitOffset,
                splitLength,
                types,
                decompressor,
                encryptionLibrary,
                dwrfEncryptionGroupMap,
                intermediateKeyMetadata,
                rowsInRowGroup,
                hiveStorageTimeZone,
                hiveWriterVersion,
                metadataReader,
                options.getMaxMergeDistance(),
                options.getTinyStripeThreshold(),
                options.getMaxBlockSize(),
                userMetadata,
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                stripeMetadataSource,
                cacheable,
                runtimeStats);
    }

    public int nextBatch()
            throws IOException
    {
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return batchSize;
        }

        for (BatchStreamReader column : getStreamReaders()) {
            if (column != null) {
                column.prepareNextRead(batchSize);
            }
        }
        batchRead(batchSize);

        validateWritePageChecksum(batchSize);
        return batchSize;
    }

    public Block readBlock(int columnIndex)
            throws IOException
    {
        Block block = getStreamReaders()[columnIndex].readBlock();
        updateMaxCombinedBytesPerRow(columnIndex, block);
        return block;
    }

    private void validateWritePageChecksum(int batchSize)
            throws IOException
    {
        if (shouldValidateWritePageChecksum()) {
            Block[] blocks = new Block[getStreamReaders().length];
            for (int columnIndex = 0; columnIndex < getStreamReaders().length; columnIndex++) {
                blocks[columnIndex] = readBlock(columnIndex);
            }
            Page page = new Page(batchSize, blocks);
            validateWritePageChecksum(page);
        }
    }

    private static BatchStreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            Map<Integer, Type> includedColumns,
            OrcAggregatedMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        OrcType rowType = types.get(0);
        BatchStreamReader[] streamReaders = new BatchStreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                Type type = includedColumns.get(columnId);
                if (type != null) {
                    StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                    streamReaders[columnId] = BatchStreamReaders.createStreamReader(type, streamDescriptor, hiveStorageTimeZone, options, systemMemoryContext);
                }
            }
        }
        return streamReaders;
    }
}
