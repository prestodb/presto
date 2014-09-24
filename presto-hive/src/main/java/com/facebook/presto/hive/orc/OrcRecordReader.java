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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.metadata.ColumnStatistics;
import com.facebook.presto.hive.orc.metadata.CompressionKind;
import com.facebook.presto.hive.orc.metadata.MetadataReader;
import com.facebook.presto.hive.orc.metadata.StripeInformation;
import com.facebook.presto.hive.orc.metadata.StripeStatistics;
import com.facebook.presto.hive.orc.metadata.Type;
import com.facebook.presto.hive.orc.metadata.Type.Kind;
import com.facebook.presto.hive.orc.reader.StreamReader;
import com.facebook.presto.hive.orc.reader.StreamReaders;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.orc.OrcDomainExtractor.extractDomain;
import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class OrcRecordReader
{
    private final OrcDataSource orcDataSource;

    private final StreamReader[] streamReaders;

    private final long totalRowCount;
    private final long splitLength;
    private long currentPosition;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private int currentStripe = -1;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private long currentGroupRowCount;
    private long nextRowInGroup;

    private final Map<HiveColumnHandle, Integer> columnHandleStreamIndex;

    public OrcRecordReader(
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            TupleDomain<HiveColumnHandle> tupleDomain,
            List<HiveColumnHandle> columnHandles,
            List<Type> types,
            CompressionKind compressionKind,
            int bufferSize,
            long rowIndexStride,
            DateTimeZone hiveStorageTimeZone,
            DateTimeZone sessionTimeZone,
            MetadataReader metadataReader,
            TypeManager typeManager)
            throws IOException
    {
        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        // it is possible that old versions of orc use 0 to mean there are no strides
        checkArgument(rowIndexStride > 0, "rowIndexStride must be greater than zero");

        boolean[] includedStreams = findIncludedStreams(types, columnHandles);

        columnHandleStreamIndex = getColumnHandleStreamIndex(types, columnHandles);

        long totalRowCount = 0;
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        if (doesFileOverlapTupleDomain(typeManager, numberOfRows, fileStats, tupleDomain, columnHandleStreamIndex)) {
            // select stripes that start within the specified split
            long splitEndOffset = splitOffset + splitLength;
            for (int i = 0; i < fileStripes.size(); i++) {
                StripeInformation stripe = fileStripes.get(i);
                if (splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset) {
                    boolean overlaps = doesStripOverlapTupleDomain(typeManager, stripe, stripeStats, tupleDomain, columnHandleStreamIndex, i);
                    if (overlaps) {
                        stripes.add(stripe);
                        totalRowCount += stripe.getNumberOfRows();
                    }
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
                includedStreams,
                rowIndexStride,
                columnHandleStreamIndex,
                tupleDomain,
                metadataReader,
                typeManager);

        streamReaders = createStreamReaders(this.orcDataSource, types, hiveStorageTimeZone, sessionTimeZone, includedStreams);
    }

    private static boolean doesFileOverlapTupleDomain(
            TypeManager typeManager,
            long numberOfRows,
            List<ColumnStatistics> columnStats,
            TupleDomain<HiveColumnHandle> tupleDomain,
            Map<HiveColumnHandle, Integer> columnHandleStreamIndex)
    {
        TupleDomain<HiveColumnHandle> stripeDomain = extractDomain(typeManager, columnHandleStreamIndex, numberOfRows, columnStats);
        return tupleDomain.overlaps(stripeDomain);
    }

    private static boolean doesStripOverlapTupleDomain(
            TypeManager typeManager,
            StripeInformation stripe,
            List<StripeStatistics> stripeStats,
            TupleDomain<HiveColumnHandle> tupleDomain,
            Map<HiveColumnHandle, Integer> columnHandleStreamIndex,
            int i)
    {
        // if there are no stats, include the column
        if (stripeStats.size() <= i) {
            return true;
        }

        List<ColumnStatistics> columnStats = stripeStats.get(i).getColumnStatistics();
        TupleDomain<HiveColumnHandle> stripeDomain = extractDomain(typeManager, columnHandleStreamIndex, stripe.getNumberOfRows(), columnStats);
        return tupleDomain.overlaps(stripeDomain);
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

    public boolean isColumnPresent(HiveColumnHandle columnHandle)
    {
        return columnHandleStreamIndex.containsKey(columnHandle);
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
                column.setNextBatchSize(batchSize);
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

    private static Map<HiveColumnHandle, Integer> getColumnHandleStreamIndex(List<Type> types, List<HiveColumnHandle> columns)
    {
        ImmutableMap.Builder<HiveColumnHandle, Integer> builder = ImmutableMap.builder();

        Type root = types.get(0);
        for (HiveColumnHandle column : columns) {
            if (!column.isPartitionKey() && column.getHiveColumnIndex() < root.getFieldCount()) {
                builder.put(column, root.getFieldTypeIndex(column.getHiveColumnIndex()));
            }
        }

        return builder.build();
    }

    private static boolean[] findIncludedStreams(List<Type> types, List<HiveColumnHandle> columns)
    {
        boolean[] includes = new boolean[types.size()];

        Type root = types.get(0);
        List<Integer> included = Lists.transform(columns, HiveColumnHandle.hiveColumnIndexGetter());
        for (int i = 0; i < root.getFieldCount(); ++i) {
            if (included.contains(i)) {
                includeStreamRecursive(types, includes, root.getFieldTypeIndex(i));
            }
        }

        return includes;
    }

    private static void includeStreamRecursive(List<Type> types, boolean[] result, int typeId)
    {
        result[typeId] = true;
        Type type = types.get(typeId);
        int children = type.getFieldCount();
        for (int i = 0; i < children; ++i) {
            includeStreamRecursive(types, result, type.getFieldTypeIndex(i));
        }
    }

    public static StreamReader[] createStreamReaders(OrcDataSource orcDataSource,
            List<Type> types,
            DateTimeZone hiveStorageTimeZone,
            DateTimeZone sessionTimeZone,
            boolean[] includedStreams)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        Type rowType = types.get(0);
        StreamReader[] streamReaders = new StreamReader[rowType.getFieldCount()];
        for (int fieldId = 0; fieldId < rowType.getFieldCount(); fieldId++) {
            int streamId = rowType.getFieldTypeIndex(fieldId);
            if (includedStreams == null || includedStreams[streamId]) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(fieldId);
                streamReaders[fieldId] = StreamReaders.createStreamReader(streamDescriptor, hiveStorageTimeZone, sessionTimeZone);
            }
        }
        return streamReaders;
    }

    private static StreamDescriptor createStreamDescriptor(String parentStreamName, String fieldName, int typeId, List<Type> types, OrcDataSource dataSource)
    {
        Type type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreams = ImmutableList.builder();
        if (type.getKind() == Kind.STRUCT) {
            for (int i = 0; i < type.getFieldCount(); ++i) {
                nestedStreams.add(createStreamDescriptor(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, dataSource));
            }
        }
        else if (type.getKind() == Kind.LIST) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "item", type.getFieldTypeIndex(0), types, dataSource));
        }
        else if (type.getKind() == Kind.MAP) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "key", type.getFieldTypeIndex(0), types, dataSource));
            nestedStreams.add(createStreamDescriptor(parentStreamName, "value", type.getFieldTypeIndex(1), types, dataSource));
        }
        return new StreamDescriptor(parentStreamName, typeId, fieldName, type.getKind(), dataSource, nestedStreams.build());
    }
}
