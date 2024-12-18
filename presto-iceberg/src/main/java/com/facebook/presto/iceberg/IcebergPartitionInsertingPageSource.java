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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static com.facebook.presto.iceberg.IcebergUtil.deserializePartitionValue;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This class is responsible for filtering the set of columns that should be
 * read by a page source which performs IO to on-disk files to only those which
 * are not partition or metadata fields.
 * <p>
 * When a new page is requested, this class inserts the partition fields as
 * RLE-encoded blocks to save memory and IO cost at runtime.
 */
public class IcebergPartitionInsertingPageSource
        implements ConnectorPageSource
{
    private final IcebergColumnHandle[] nonPartitionColumnIndexes;
    private final ConnectorPageSourceWithRowPositions delegateWithPositions;
    private final ConnectorPageSource delegate;
    private final Block[] partitionValueBlocks;
    private final long partitionValuesMemoryUsage;
    // maps output array index to index of input page from delegate provider.
    private final int[] outputIndexes;

    public IcebergPartitionInsertingPageSource(
            List<IcebergColumnHandle> fullColumnList,
            Map<Integer, Object> metadataValues,
            Map<Integer, HivePartitionKey> partitionKeys,
            Function<List<IcebergColumnHandle>, ConnectorPageSourceWithRowPositions> delegateSupplier)
    {
        this.nonPartitionColumnIndexes = new IcebergColumnHandle[fullColumnList.size()];
        this.outputIndexes = new int[fullColumnList.size()];
        populateIndexes(fullColumnList, partitionKeys);
        this.partitionValueBlocks = generatePartitionValueBlocks(fullColumnList, metadataValues, partitionKeys);
        this.partitionValuesMemoryUsage = Arrays.stream(partitionValueBlocks).filter(Objects::nonNull).mapToLong(Block::getRetainedSizeInBytes).sum();

        List<IcebergColumnHandle> delegateColumns = Arrays.stream(nonPartitionColumnIndexes)
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        this.delegateWithPositions = delegateSupplier.apply(delegateColumns);
        this.delegate = delegateWithPositions.getDelegate();
    }

    private void populateIndexes(List<IcebergColumnHandle> fullColumnList, Map<Integer, HivePartitionKey> partitionKeys)
    {
        int delegateIndex = 0;
        // generate array of non-partition column indexes
        for (int i = 0; i < fullColumnList.size(); i++) {
            IcebergColumnHandle handle = fullColumnList.get(i);
            if (handle.getColumnType() == PARTITION_KEY ||
                    partitionKeys.containsKey(handle.getId()) ||
                    isMetadataColumnId(handle.getId())) {
                // is partition key, don't include for delegate supplier
                continue;
            }
            nonPartitionColumnIndexes[i] = handle;
            outputIndexes[i] = delegateIndex;
            delegateIndex++;
        }
    }

    private Block[] generatePartitionValueBlocks(
            List<IcebergColumnHandle> fullColumnList,
            Map<Integer, Object> metadataValues,
            Map<Integer, HivePartitionKey> partitionKeys)
    {
        return IntStream.range(0, fullColumnList.size())
                .mapToObj(index -> {
                    IcebergColumnHandle column = fullColumnList.get(index);
                    if (nonPartitionColumnIndexes[index] != null) {
                        return null;
                    }

                    Type type = column.getType();
                    if (partitionKeys.containsKey(column.getId())) {
                        HivePartitionKey icebergPartition = partitionKeys.get(column.getId());
                        Object prefilledValue = deserializePartitionValue(type, icebergPartition.getValue().orElse(null), column.getName());
                        return nativeValueToBlock(type, prefilledValue);
                    }
                    else if (column.getColumnType() == PARTITION_KEY) {
                        // Partition key with no value. This can happen after partition evolution
                        return nativeValueToBlock(type, null);
                    }
                    else if (isMetadataColumnId(column.getId())) {
                        return nativeValueToBlock(type, metadataValues.get(column.getColumnIdentity().getId()));
                    }

                    return null;
                })
                .toArray(Block[]::new);
    }

    public ConnectorPageSourceWithRowPositions getRowPositionDelegate()
    {
        return delegateWithPositions;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }
            int batchSize = dataPage.getPositionCount();
            Block[] blocks = new Block[nonPartitionColumnIndexes.length];
            for (int i = 0; i < nonPartitionColumnIndexes.length; i++) {
                blocks[i] = partitionValueBlocks[i] == null ?
                        dataPage.getBlock(outputIndexes[i]) :
                        new RunLengthEncodedBlock(partitionValueBlocks[i], batchSize);
            }

            return new Page(batchSize, blocks);
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, PrestoException.class);
            throw new PrestoException(ICEBERG_BAD_DATA, e);
        }
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage() + partitionValuesMemoryUsage;
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
