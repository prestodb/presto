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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.SelectiveStreamReader;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.reader.SelectiveStreamReaders.createStreamReader;

public class OrcSelectiveRecordReader
        extends AbstractOrcRecordReader<SelectiveStreamReader>
{
    private final List<Integer> outputColumns;

    private int[] positions;

    public OrcSelectiveRecordReader(
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            Map<Integer, TupleDomainFilter> filters,
            Map<Integer, List<Subfield>> requiredSubfields,
            Map<Integer, Object> constantValues,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long offset,
            long length,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            PostScript.HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize)
    {
        super(includedColumns,
                createStreamReaders(
                        orcDataSource,
                        types,
                        hiveStorageTimeZone,
                        includedColumns,
                        outputColumns,
                        filters,
                        requiredSubfields,
                        systemMemoryUsage.newAggregatedMemoryContext()),
                predicate,
                numberOfRows,
                fileStripes,
                fileStats,
                stripeStats,
                orcDataSource,
                offset,
                length,
                types,
                decompressor,
                rowsInRowGroup,
                hiveStorageTimeZone,
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                userMetadata,
                systemMemoryUsage,
                writeValidation,
                initialBatchSize);

        this.outputColumns = outputColumns;
    }

    private static SelectiveStreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            Map<Integer, Type> includedColumns,
            List<Integer> outputColumns,
            Map<Integer, TupleDomainFilter> filters,
            Map<Integer, List<Subfield>> requiredSubfields,
            AggregatedMemoryContext systemMemoryContext)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        OrcType rowType = types.get(0);
        SelectiveStreamReader[] streamReaders = new SelectiveStreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                streamReaders[columnId] = createStreamReader(
                        streamDescriptor,
                        Optional.ofNullable(filters.get(columnId)),
                        outputColumns.contains(columnId),
                        Optional.ofNullable(requiredSubfields.get(columnId)).orElse(ImmutableList.of()),
                        hiveStorageTimeZone,
                        systemMemoryContext);
            }
        }
        return streamReaders;
    }

    public Page getNextPage()
            throws IOException
    {
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        initializePositions(batchSize);

        int[] positionsToRead = this.positions;
        int positionCount = batchSize;
        for (SelectiveStreamReader streamReader : getStreamReaders()) {
            if (streamReader != null) {
                positionCount = streamReader.read(getNextRowInGroup(), positionsToRead, positionCount);
                if (positionCount == 0) {
                    break;
                }

                positionsToRead = streamReader.getReadPositions();
            }
        }

        batchRead(batchSize);

        if (positionCount == 0) {
            return new Page(0);
        }

        Block[] blocks = new Block[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            int columnIndex = outputColumns.get(i);
            Block block = getStreamReaders()[columnIndex].getBlock(positionsToRead, positionCount);
            updateMaxCombinedBytesPerRow(columnIndex, block);
            blocks[i] = block;
        }

        Page page = new Page(positionCount, blocks);

        validateWritePageChecksum(page);

        return page;
    }

    private void initializePositions(int batchSize)
    {
        if (positions == null || positions.length < batchSize) {
            positions = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
                positions[i] = i;
            }
        }
    }
}
