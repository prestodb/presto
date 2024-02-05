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
import com.facebook.presto.common.Utils;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.iceberg.delete.RowPredicate;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergUtil.deserializePartitionValue;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class IcebergPageSource
        implements ConnectorPageSource
{
    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final ConnectorPageSource delegate;
    private final Supplier<Optional<RowPredicate>> deletePredicate;

    public IcebergPageSource(
            List<IcebergColumnHandle> columns,
            Map<Integer, Object> metadataValues,
            Map<Integer, HivePartitionKey> partitionKeys,
            ConnectorPageSource delegate,
            Supplier<Optional<RowPredicate>> deletePredicate)
    {
        int size = requireNonNull(columns, "columns is null").size();
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.delegate = requireNonNull(delegate, "delegate is null");

        this.deletePredicate = requireNonNull(deletePredicate, "deletePredicate is null");

        prefilledBlocks = new Block[size];
        delegateIndexes = new int[size];

        int outputIndex = 0;
        int delegateIndex = 0;
        for (IcebergColumnHandle column : columns) {
            if (partitionKeys.containsKey(column.getId())) {
                HivePartitionKey icebergPartition = partitionKeys.get(column.getId());
                Type type = column.getType();
                Object prefilledValue = deserializePartitionValue(type, icebergPartition.getValue().orElse(null), column.getName());
                prefilledBlocks[outputIndex] = nativeValueToBlock(type, prefilledValue);
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getColumnType() == PARTITION_KEY) {
                // Partition key with no value. This can happen after partition evolution
                Type type = column.getType();
                prefilledBlocks[outputIndex] = nativeValueToBlock(type, null);
                delegateIndexes[outputIndex] = -1;
            }
            else if (IcebergMetadataColumn.isMetadataColumnId(column.getId())) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(column.getType(), metadataValues.get(column.getColumnIdentity().getId()));
                delegateIndexes[outputIndex] = -1;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            outputIndex++;
        }
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

            Optional<RowPredicate> deleteFilterPredicate = deletePredicate.get();
            if (deleteFilterPredicate.isPresent()) {
                dataPage = deleteFilterPredicate.get().filterPage(dataPage);
            }

            int batchSize = dataPage.getPositionCount();
            Block[] blocks = new Block[prefilledBlocks.length];
            for (int i = 0; i < prefilledBlocks.length; i++) {
                if (prefilledBlocks[i] != null) {
                    blocks[i] = new RunLengthEncodedBlock(prefilledBlocks[i], batchSize);
                }
                else {
                    blocks[i] = dataPage.getBlock(delegateIndexes[i]);
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throwIfInstanceOf(e, PrestoException.class);
            throw new PrestoException(ICEBERG_BAD_DATA, e);
        }
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

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
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

    private Block nativeValueToBlock(Type type, Object prefilledValue)
    {
        if (prefilledValue != null && (type instanceof TimestampType && ((TimestampType) type).getPrecision() == MILLISECONDS || type instanceof TimeType)) {
            return Utils.nativeValueToBlock(type, MICROSECONDS.toMillis((long) prefilledValue));
        }
        return Utils.nativeValueToBlock(type, prefilledValue);
    }
}
