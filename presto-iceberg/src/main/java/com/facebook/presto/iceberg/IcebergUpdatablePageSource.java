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
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.iceberg.delete.IcebergDeletePageSink;
import com.facebook.presto.iceberg.delete.RowPredicate;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static com.facebook.presto.iceberg.IcebergUtil.deserializePartitionValue;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergUpdatablePageSource
        implements UpdatablePageSource
{
    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final ConnectorPageSource delegate;
    private final Supplier<IcebergDeletePageSink> deleteSinkSupplier;
    private IcebergDeletePageSink positionDeleteSink;
    private final Supplier<Optional<RowPredicate>> deletePredicate;

    private final List<IcebergColumnHandle> columns;
    private final List<IcebergColumnHandle> updatedColumns;
    private final Schema tableSchema;
    private final Supplier<IcebergPageSink> updatedRowPageSinkSupplier;
    private IcebergPageSink updatedRowPageSink;
    // An array with one element per field in the $row_id column. The value in the array points to the
    // channel where the data can be read from.
    private int[] updateRowIdChildColumnIndexes = new int[0];
    // The $row_id's index in 'expectedColumns', or -1 if there isn't one
    private int updateRowIdColumnIndex = -1;
    // Maps the Iceberg field ids of unmodified columns to their indexes in updateRowIdChildColumnIndexes
    private Map<Integer, Integer> icebergIdToRowIdColumnIndex = ImmutableMap.of();
    // Maps the Iceberg field ids of modified columns to their indexes in the updateColumns columnValueAndRowIdChannels array
    private Map<Integer, Integer> icebergIdToUpdatedColumnIndex = ImmutableMap.of();
    private final int[] expectedColumnIndexes;

    public IcebergUpdatablePageSource(
            Schema tableSchema,
            List<IcebergColumnHandle> columns,
            Map<Integer, Object> metadataValues,
            Map<Integer, HivePartitionKey> partitionKeys,
            ConnectorPageSource delegate,
            Supplier<IcebergDeletePageSink> deleteSinkSupplier,
            Supplier<Optional<RowPredicate>> deletePredicate,
            Supplier<IcebergPageSink> updatedRowPageSinkSupplier,
            List<IcebergColumnHandle> updatedColumns,
            List<IcebergColumnHandle> requiredColumns)
    {
        this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
        this.columns = requireNonNull(columns, "columns is null");
        int size = this.columns.size();
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.deleteSinkSupplier = deleteSinkSupplier;

        this.deletePredicate = requireNonNull(deletePredicate, "deletePredicate is null");

        prefilledBlocks = new Block[size];
        delegateIndexes = new int[size];
        this.updatedRowPageSinkSupplier = requireNonNull(updatedRowPageSinkSupplier, "updatedRowPageSinkSupplier is null");
        this.updatedColumns = ImmutableList.copyOf(requireNonNull(updatedColumns, "updatedColumns is null"));

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
            else if (isMetadataColumnId(column.getId())) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(column.getType(), metadataValues.get(column.getColumnIdentity().getId()));
                delegateIndexes[outputIndex] = -1;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
            }
            outputIndex++;
        }

        this.expectedColumnIndexes = new int[requiredColumns.size()];
        if (!requiredColumns.isEmpty()) {
            for (int columnIndex = 0; columnIndex < requiredColumns.size(); columnIndex++) {
                expectedColumnIndexes[columnIndex] = columnIndex;
            }
        }

        if (!columns.isEmpty()) {
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                IcebergColumnHandle expectedColumn = columns.get(columnIndex);
                if (expectedColumn.isUpdateRowIdColumn()) {
                    this.updateRowIdColumnIndex = columnIndex;
                    Map<Integer, Integer> fieldIdToColumnIndex = mapFieldIdsToIndex(requiredColumns);
                    List<ColumnIdentity> rowIdFields = expectedColumn.getColumnIdentity().getChildren();
                    ImmutableMap.Builder<Integer, Integer> fieldIdToRowIdIndex = ImmutableMap.builder();
                    this.updateRowIdChildColumnIndexes = new int[rowIdFields.size()];
                    for (int i = 0; i < rowIdFields.size(); i++) {
                        int fieldId = rowIdFields.get(i).getId();
                        updateRowIdChildColumnIndexes[i] = requireNonNull(fieldIdToColumnIndex.get(fieldId), () -> format("Column %s not found in requiredColumns", fieldId));
                        fieldIdToRowIdIndex.put(fieldId, i);
                    }
                    this.icebergIdToRowIdColumnIndex = fieldIdToRowIdIndex.buildOrThrow();
                }
            }
        }

        if (!updatedColumns.isEmpty()) {
            ImmutableMap.Builder<Integer, Integer> icebergIdToUpdatedColumnIndex = ImmutableMap.builder();
            for (int columnIndex = 0; columnIndex < updatedColumns.size(); columnIndex++) {
                IcebergColumnHandle updatedColumn = updatedColumns.get(columnIndex);
                icebergIdToUpdatedColumnIndex.put(updatedColumn.getId(), columnIndex);
            }
            this.icebergIdToUpdatedColumnIndex = icebergIdToUpdatedColumnIndex.buildOrThrow();
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

            if (!updatedColumns.isEmpty()) {
                dataPage = setUpdateRowIdBlock(dataPage);
                dataPage = dataPage.extractChannels(expectedColumnIndexes);
                return dataPage;
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
    public void deleteRows(Block rowIds)
    {
        if (positionDeleteSink == null) {
            positionDeleteSink = deleteSinkSupplier.get();
        }
        positionDeleteSink.appendPage(new Page(rowIds));
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        int rowIdChannel = columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1);
        List<Integer> columnChannelMapping = columnValueAndRowIdChannels.subList(0, columnValueAndRowIdChannels.size() - 1);

        if (positionDeleteSink == null) {
            positionDeleteSink = deleteSinkSupplier.get();
            verify(positionDeleteSink != null);
        }
        if (updatedRowPageSink == null) {
            updatedRowPageSink = updatedRowPageSinkSupplier.get();
            verify(updatedRowPageSink != null);
        }

        ColumnarRow rowIdColumns = toColumnarRow(page.getBlock(rowIdChannel));
        positionDeleteSink.appendPage(new Page(rowIdColumns.getField(0)));

        Set<Integer> updatedColumnFieldIds = icebergIdToUpdatedColumnIndex.keySet();
        List<Types.NestedField> tableColumns = tableSchema.columns();
        Block[] fullPage = new Block[tableColumns.size()];
        for (int targetChannel = 0; targetChannel < tableColumns.size(); targetChannel++) {
            Types.NestedField column = tableColumns.get(targetChannel);
            if (updatedColumnFieldIds.contains(column.fieldId())) {
                fullPage[targetChannel] = page.getBlock(columnChannelMapping.get(icebergIdToUpdatedColumnIndex.get(column.fieldId())));
            }
            else {
                // Plus one because the first field is the row position column
                fullPage[targetChannel] = rowIdColumns.getField(icebergIdToRowIdColumnIndex.get(column.fieldId()));
            }
        }
        updatedRowPageSink.appendPage(new Page(page.getPositionCount(), fullPage));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return Optional.ofNullable(positionDeleteSink)
                .map(IcebergDeletePageSink::finish)
                .orElseGet(() -> CompletableFuture.completedFuture(ImmutableList.of()))
                .thenCombine(
                        Optional.ofNullable(updatedRowPageSink).map(IcebergPageSink::finish)
                                .orElseGet(() -> CompletableFuture.completedFuture(ImmutableList.of())),
                        (positionDeletes, writtenFiles) -> ImmutableList.<Slice>builder()
                                .addAll(positionDeletes)
                                .addAll(writtenFiles)
                                .build());
    }

    @Override
    public void abort()
    {
        if (positionDeleteSink != null) {
            positionDeleteSink.abort();
        }

        if (updatedRowPageSink != null) {
            updatedRowPageSink.abort();
        }
    }

    /**
     * The $row_id column used for updates is a composite column of at least one other column in the Page.
     * The indexes of the columns needed for the $row_id are in the updateRowIdChildColumnIndexes array.
     *
     * @param page The raw Page from the Parquet/ORC reader.
     * @return A Page where the $row_id channel has been populated.
     */
    private Page setUpdateRowIdBlock(Page page)
    {
        if (updateRowIdColumnIndex == -1) {
            return page;
        }

        Block[] rowIdFields = new Block[updateRowIdChildColumnIndexes.length];
        for (int childIndex = 0; childIndex < updateRowIdChildColumnIndexes.length; childIndex++) {
            rowIdFields[childIndex] = page.getBlock(updateRowIdChildColumnIndexes[childIndex]);
        }

        Block[] fullPage = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel == updateRowIdColumnIndex) {
                fullPage[channel] = RowBlock.fromFieldBlocks(page.getPositionCount(), Optional.empty(), rowIdFields);
                continue;
            }
            fullPage[channel] = page.getBlock(channel);
        }

        return new Page(page.getPositionCount(), fullPage);
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
        long totalMemUsage = delegate.getSystemMemoryUsage();
        if (positionDeleteSink != null) {
            totalMemUsage += positionDeleteSink.getSystemMemoryUsage();
        }

        if (updatedRowPageSink != null) {
            totalMemUsage += updatedRowPageSink.getSystemMemoryUsage();
        }

        return totalMemUsage;
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

    private static Map<Integer, Integer> mapFieldIdsToIndex(List<IcebergColumnHandle> columns)
    {
        ImmutableMap.Builder<Integer, Integer> fieldIdsToIndex = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            fieldIdsToIndex.put(columns.get(i).getId(), i);
        }
        return fieldIdsToIndex.buildOrThrow();
    }
}
