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

import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.StripeStatistics;
import com.facebook.presto.orc.reader.StreamReader;
import com.facebook.presto.orc.reader.StreamReaders;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcRecordReader
{
    private final OrcDataSource orcDataSource;

    private final StreamReader[] streamReaders;

    private final long totalRowCount;
    private final long splitLength;
    private final Set<Integer> presentColumns;
    private long currentPosition;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private int currentStripe = -1;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private long currentGroupRowCount;
    private long nextRowInGroup;

    public OrcRecordReader(
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
            CompressionKind compressionKind,
            int bufferSize,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            MetadataReader metadataReader)
            throws IOException
    {
        checkNotNull(includedColumns, "includedColumns is null");
        checkNotNull(predicate, "predicate is null");
        checkNotNull(fileStripes, "fileStripes is null");
        checkNotNull(stripeStats, "stripeStats is null");
        checkNotNull(orcDataSource, "orcDataSource is null");
        checkNotNull(types, "types is null");
        checkNotNull(compressionKind, "compressionKind is null");
        checkNotNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");

        // reduce the included columns to the set that is also present
        ImmutableSet.Builder<Integer> presentColumns = ImmutableSet.builder();
        ImmutableMap.Builder<Integer, Type> presentColumnsAndTypes = ImmutableMap.builder();
        OrcType root = types.get(0);
        for (Map.Entry<Integer, Type> entry : includedColumns.entrySet()) {
            // an old file can have less columns since columns can be added
            // after the file was written
            if (entry.getKey() < root.getFieldCount()) {
                presentColumns.add(entry.getKey());
                presentColumnsAndTypes.put(entry.getKey(), entry.getValue());
            }
        }
        this.presentColumns = presentColumns.build();

        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        // it is possible that old versions of orc use 0 to mean there are no row groups
        checkArgument(rowsInRowGroup > 0, "rowsInRowGroup must be greater than zero");

        long totalRowCount = 0;
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        if (predicate.matches(numberOfRows, getStatisticsByColumnOrdinal(root, fileStats))) {
            // select stripes that start within the specified split
            for (int stripeIndex = 0; stripeIndex < fileStripes.size(); stripeIndex++) {
                StripeInformation stripe = fileStripes.get(stripeIndex);
                if (splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(root, stripe, stripeStats, predicate, stripeIndex)) {
                    stripes.add(stripe);
                    totalRowCount += stripe.getNumberOfRows();
                }
            }
        }
        this.totalRowCount = totalRowCount;
        this.stripes = stripes.build();

        stripeReader = new StripeReader(
                orcDataSource,
                compressionKind,
                types,
                bufferSize,
                this.presentColumns,
                rowsInRowGroup,
                predicate,
                metadataReader);

        streamReaders = createStreamReaders(orcDataSource, types, hiveStorageTimeZone, presentColumnsAndTypes.build());
    }

    private static boolean splitContainsStripe(long splitOffset, long splitLength, StripeInformation stripe)
    {
        long splitEndOffset = splitOffset + splitLength;
        return splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset;
    }

    private static boolean isStripeIncluded(
            OrcType rootStructType,
            StripeInformation stripe,
            List<StripeStatistics> stripeStats,
            OrcPredicate predicate,
            int stripeIndex)
    {
        // if there are no stats, include the column
        if (stripeIndex >= stripeStats.size()) {
            return true;
        }

        return predicate.matches(stripe.getNumberOfRows(), getStatisticsByColumnOrdinal(rootStructType, stripeStats.get(stripeIndex).getColumnStatistics()));
    }

    public long getPosition()
    {
        return currentPosition;
    }

    public long getTotalRowCount()
    {
        return totalRowCount;
    }

    public float getProgress()
    {
        return ((float) currentPosition) / totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    public void close()
            throws IOException
    {
        orcDataSource.close();
    }

    public boolean isColumnPresent(int hiveColumnIndex)
    {
        return presentColumns.contains(hiveColumnIndex);
    }

    public int nextBatch()
            throws IOException
    {
        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                return -1;
            }
        }

        int batchSize = Ints.checkedCast(Math.min(Vector.MAX_VECTOR_LENGTH, currentGroupRowCount - nextRowInGroup));

        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.prepareNextRead(batchSize);
            }
        }
        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        return batchSize;
    }

    public void readVector(int columnIndex, Object vector)
            throws IOException
    {
        streamReaders[columnIndex].readBatch(vector);
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        nextRowInGroup = 0;

        while (!rowGroups.hasNext() && currentStripe < stripes.size()) {
            advanceToNextStripe();
        }

        if (!rowGroups.hasNext()) {
            currentGroupRowCount = 0;
            return false;
        }

        RowGroup currentRowGroup = rowGroups.next();
        currentGroupRowCount = currentRowGroup.getRowCount();

        // give reader data streams from row group
        StreamSources rowGroupStreamSources = currentRowGroup.getStreamSources();
        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.startRowGroup(rowGroupStreamSources);
            }
        }

        return true;
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripe++;
        if (currentStripe >= stripes.size()) {
            return;
        }

        StripeInformation stripeInformation = stripes.get(currentStripe);
        Stripe stripe = stripeReader.readStripe(stripeInformation);
        if (stripe != null) {
            // Give readers access to dictionary streams
            StreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
            List<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    column.startStripe(dictionaryStreamSources, columnEncodings);
                }
            }

            rowGroups = stripe.getRowGroups().iterator();
        }
        else {
            rowGroups = ImmutableList.<RowGroup>of().iterator();
        }
    }

    private static StreamReader[] createStreamReaders(OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            Map<Integer, Type> includedColumns)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        OrcType rowType = types.get(0);
        StreamReader[] streamReaders = new StreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                streamReaders[columnId] = StreamReaders.createStreamReader(streamDescriptor, includedColumns.get(columnId), hiveStorageTimeZone);
            }
        }
        return streamReaders;
    }

    private static StreamDescriptor createStreamDescriptor(String parentStreamName, String fieldName, int typeId, List<OrcType> types, OrcDataSource dataSource)
    {
        OrcType type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreams = ImmutableList.builder();
        if (type.getOrcTypeKind() == OrcTypeKind.STRUCT) {
            for (int i = 0; i < type.getFieldCount(); ++i) {
                nestedStreams.add(createStreamDescriptor(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, dataSource));
            }
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.LIST) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "item", type.getFieldTypeIndex(0), types, dataSource));
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.MAP) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "key", type.getFieldTypeIndex(0), types, dataSource));
            nestedStreams.add(createStreamDescriptor(parentStreamName, "value", type.getFieldTypeIndex(1), types, dataSource));
        }
        return new StreamDescriptor(parentStreamName, typeId, fieldName, type.getOrcTypeKind(), dataSource, nestedStreams.build());
    }

    private static Map<Integer, ColumnStatistics> getStatisticsByColumnOrdinal(OrcType rootStructType, List<ColumnStatistics> fileStats)
    {
        checkNotNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        checkNotNull(fileStats, "fileStats is null");

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < rootStructType.getFieldCount(); ordinal++) {
            ColumnStatistics element = fileStats.get(rootStructType.getFieldTypeIndex(ordinal));
            if (element != null) {
                statistics.put(ordinal, element);
            }
        }
        return statistics.build();
    }
}
