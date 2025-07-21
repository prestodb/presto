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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.iceberg.delete.DeleteFilter;
import com.facebook.presto.iceberg.delete.IcebergDeletePageSink;
import com.facebook.presto.iceberg.delete.RowPredicate;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_MISSING_COLUMN;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.UnaryOperator.identity;

/**
 * Generates pages for an iceberg table while applying delete filters to rows
 * _and_ by modifying the format of the output pages for update operators if
 * required.
 */
public class IcebergUpdateablePageSource
        implements UpdatablePageSource
{
    private final ConnectorPageSource delegate;
    private final Supplier<IcebergDeletePageSink> deleteSinkSupplier;
    private IcebergDeletePageSink positionDeleteSink;
    private final Supplier<Optional<RowPredicate>> deletePredicate;
    private final Supplier<List<DeleteFilter>> deleteFilters;

    private final List<IcebergColumnHandle> columns;
    /**
     * Columns actually updated in the query
     */
    private final List<IcebergColumnHandle> updatedColumns;
    private final List<IcebergColumnHandle> delegateColumns;
    private final Schema tableSchema;
    private final Supplier<IcebergPageSink> updatedRowPageSinkSupplier;
    private IcebergPageSink updatedRowPageSink;
    // An array with one element per field in the $row_id column. The value in the array points to the
    // channel where the data can be read from within the input page
    private final int[] updateRowIdChildColumnIndexes;
    // The $row_id's index in 'outputColumns', or -1 if there isn't one
    private final int updateRowIdColumnIndex;
    // Maps the Iceberg field ids of unmodified columns to their indexes in updateRowIdChildColumnIndexes
    private final Map<ColumnIdentity, Integer> columnIdToRowIdColumnIndex = new HashMap<>();
    // Maps the Iceberg field ids of modified columns to their indexes in the updatedColumns columnValueAndRowIdChannels array
    private final Map<ColumnIdentity, Integer> columnIdentityToUpdatedColumnIndex = new HashMap<>();
    private final int[] outputColumnToDelegateMapping;
    private final int isDeletedColumnId;
    private final int deleteFilePathColumnId;

    public IcebergUpdateablePageSource(
            Schema tableSchema,
            // represents the columns which need to be output from `getNextPage`
            List<IcebergColumnHandle> outputColumns,
            Map<Integer, Object> metadataValues,
            Map<Integer, HivePartitionKey> partitionKeys,
            ConnectorPageSource delegate,
            // represents the columns output by the delegate page source
            List<IcebergColumnHandle> delegateColumns,
            Supplier<IcebergDeletePageSink> deleteSinkSupplier,
            Supplier<Optional<RowPredicate>> deletePredicate,
            Supplier<List<DeleteFilter>> deleteFilters,
            Supplier<IcebergPageSink> updatedRowPageSinkSupplier,
            // the columns that this page source is supposed to update
            List<IcebergColumnHandle> updatedColumns,
            Optional<IcebergColumnHandle> updateRowIdColumn)
    {
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
        this.columns = requireNonNull(outputColumns, "columns is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delegateColumns = requireNonNull(delegateColumns, "delegateColumns is null");
        // information for deletes
        this.deleteSinkSupplier = deleteSinkSupplier;
        this.deletePredicate = requireNonNull(deletePredicate, "deletePredicate is null");
        this.deleteFilters = requireNonNull(deleteFilters, "deleteFilters is null");
        // information for updates
        this.updatedRowPageSinkSupplier = requireNonNull(updatedRowPageSinkSupplier, "updatedRowPageSinkSupplier is null");
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
        this.outputColumnToDelegateMapping = new int[columns.size()];
        this.updateRowIdColumnIndex = updateRowIdColumn.map(columns::indexOf).orElse(-1);
        this.updateRowIdChildColumnIndexes = updateRowIdColumn
                .map(column -> new int[column.getColumnIdentity().getChildren().size()])
                .orElse(new int[0]);
        Map<ColumnIdentity, Integer> columnToIndex = IntStream.range(0, delegateColumns.size())
                .boxed()
                .collect(toImmutableMap(index -> delegateColumns.get(index).getColumnIdentity(), identity()));
        updateRowIdColumn.ifPresent(column -> {
            List<ColumnIdentity> rowIdFields = column.getColumnIdentity().getChildren();
            for (int i = 0; i < rowIdFields.size(); i++) {
                ColumnIdentity columnIdentity = rowIdFields.get(i);
                updateRowIdChildColumnIndexes[i] = requireNonNull(columnToIndex.get(columnIdentity), () -> format("Column %s not found in requiredColumns", columnIdentity));
                columnIdToRowIdColumnIndex.put(columnIdentity, i);
            }
        });

        if (!updatedColumns.isEmpty()) {
            for (int columnIndex = 0; columnIndex < updatedColumns.size(); columnIndex++) {
                IcebergColumnHandle updatedColumn = updatedColumns.get(columnIndex);
                columnIdentityToUpdatedColumnIndex.put(updatedColumn.getColumnIdentity(), columnIndex);
            }
        }
        for (int i = 0; i < outputColumnToDelegateMapping.length; i++) {
            IcebergColumnHandle outputColumn = outputColumns.get(i);
            if (outputColumn.isUpdateRowIdColumn() || outputColumn.isMergeRowIdColumn()) {
                continue;
            }

            if (!columnToIndex.containsKey(outputColumn.getColumnIdentity())) {
                throw new PrestoException(ICEBERG_MISSING_COLUMN, format("Column %s not found in delegate column map", outputColumn));
            }
            else {
                outputColumnToDelegateMapping[i] = columnToIndex.get(outputColumn.getColumnIdentity());
            }
        }
        this.isDeletedColumnId = getDelegateColumnId(IcebergColumnHandle::isDeletedColumn);
        this.deleteFilePathColumnId = getDelegateColumnId(IcebergColumnHandle::isDeleteFilePathColumn);
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

    /**
     * This method takes three steps in order to generate the final output page.
     * <br>
     * 1. Retrieve rows from the delegate page source. Usually this is the source for the file
     * format, such as {@link com.facebook.presto.hive.parquet.ParquetPageSource} or
     * {@link IcebergPartitionInsertingPageSource}.
     * 2. Using the newly retrieved page, apply any necessary delete filters.
     * 3. Finally, take the necessary channels from the page with the delete filters applied and
     * nest them into the updateRowId channel in {@link #setUpdateRowIdBlock(Page)}
     */
    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            Optional<RowPredicate> deleteFilterPredicate = deletePredicate.get();
            if (isDeletedColumnId != -1 || deleteFilePathColumnId != -1) {
                if (isDeletedColumnId != -1) {
                    if (deleteFilterPredicate.isPresent()) {
                        // Instead of filtering rows, we mark whether the row is deleted in the $deleted column
                        dataPage = deleteFilterPredicate.get().markDeleted(dataPage, isDeletedColumnId);
                    }
                    else {
                        Block allFalseBlock = RunLengthEncodedBlock.create(BOOLEAN, false, dataPage.getPositionCount());
                        dataPage = dataPage.replaceColumn(isDeletedColumnId, allFalseBlock);
                    }
                }
                if (deleteFilePathColumnId != -1) {
                    dataPage = markDeleteFilePath(dataPage, deleteFilePathColumnId);
                }
            }
            else if (deleteFilterPredicate.isPresent()) {
                dataPage = deleteFilterPredicate.get().filterPage(dataPage);
            }

            return setUpdateRowIdBlock(dataPage);
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

    /**
     * @param page This page contains the following channels: <br>
     *                          - One channel for the row ID, which includes the position number of this row within the file and the values of the unmodified columns.<br>
     *                          - One additional channel for each updated column. These channels contain the new values for the updated columns.
     * @param columnValueAndRowIdChannels Channel numbers of the column values and the row ID's channel number at the end of the list.
     */
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

        Set<ColumnIdentity> updatedColumnFieldIds = columnIdentityToUpdatedColumnIndex.keySet();
        List<Types.NestedField> tableColumns = tableSchema.columns();
        Block[] fullPage = new Block[tableColumns.size()];
        // Build a page that will contain the values of the updated rows. The rows stored in the "fullPage" include both updated and non-updated field values.
        for (int targetChannel = 0; targetChannel < tableColumns.size(); targetChannel++) {
            Types.NestedField column = tableColumns.get(targetChannel);
            ColumnIdentity columnIdentity = ColumnIdentity.createColumnIdentity(column);
            if (updatedColumnFieldIds.contains(columnIdentity)) {
                fullPage[targetChannel] = page.getBlock(columnChannelMapping.get(columnIdentityToUpdatedColumnIndex.get(columnIdentity)));
            }
            else {
                fullPage[targetChannel] = rowIdColumns.getField(columnIdToRowIdColumnIndex.get(columnIdentity));
            }
        }
        updatedRowPageSink.appendPage(new Page(page.getPositionCount(), fullPage));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return Optional.ofNullable(positionDeleteSink)
                .map(IcebergDeletePageSink::finish)
                .orElseGet(() -> completedFuture(ImmutableList.of()))
                .thenCombine(
                        Optional.ofNullable(updatedRowPageSink).map(IcebergPageSink::finish)
                                .orElseGet(() -> completedFuture(ImmutableList.of())),
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
     * The $row_id column used for updates and merge is a composite column of at least one other column in the Page.
     * The indexes of the columns needed for the $row_id are in the updateRowIdChildColumnIndexes array.
     *
     * @param page The raw Page from the Parquet/ORC reader.
     * @return A Page where the $row_id channel has been populated.
     */
    private Page setUpdateRowIdBlock(Page page)
    {
        // TODO #20578: Update this method to add support for the MERGE RowId.

        Block[] fullPage = new Block[columns.size()];
        Block[] rowIdFields;
        Consumer<Integer> loopFunc;
        if (updateRowIdColumnIndex == -1 || updatedColumns.isEmpty()) {
            loopFunc = (channel) -> fullPage[channel] = page.getBlock(outputColumnToDelegateMapping[channel]);
        }
        else {
            rowIdFields = new Block[updateRowIdChildColumnIndexes.length];
            for (int childIndex = 0; childIndex < updateRowIdChildColumnIndexes.length; childIndex++) {
                rowIdFields[childIndex] = page.getBlock(updateRowIdChildColumnIndexes[childIndex]);
            }
            loopFunc = (channel) -> {
                if (channel == updateRowIdColumnIndex) {
                    fullPage[channel] = RowBlock.fromFieldBlocks(page.getPositionCount(), Optional.empty(), rowIdFields);
                }
                else {
                    fullPage[channel] = page.getBlock(outputColumnToDelegateMapping[channel]);
                }
            };
        }

        for (int channel = 0; channel < columns.size(); channel++) {
            loopFunc.accept(channel);
        }

        return new Page(page.getPositionCount(), fullPage);
    }

    private int getDelegateColumnId(Predicate<IcebergColumnHandle> columnPredicate)
    {
        int targetColumnId = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columnPredicate.test(columns.get(i))) {
                targetColumnId = i;
                break;
            }
        }
        if (targetColumnId == -1) {
            return -1;
        }
        return outputColumnToDelegateMapping[targetColumnId];
    }

    private Page markDeleteFilePath(Page page, int deleteFilePathDelegateColumnId)
    {
        List<Pair<DeleteFilter, RowPredicate>> filterPredicates = deleteFilters.get().stream()
                .map(filter -> Pair.of(filter, filter.createPredicate(delegateColumns)))
                .collect(Collectors.toList());

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return page;
        }

        boolean allSameValues = true;
        Optional<String> firstValue = getDeleteFilePath(page, 0, filterPredicates);
        BlockBuilder blockBuilder = null;
        // Build the varchar block with the deleted file path or null if the row isn't deleted
        for (int position = 1; position < positionCount; position++) {
            Optional<String> deleteFilePath = getDeleteFilePath(page, position, filterPredicates);
            if (allSameValues && !Objects.equals(firstValue.orElse(null), deleteFilePath.orElse(null))) {
                blockBuilder = VARCHAR.createBlockBuilder(null, positionCount);
                for (int idx = 0; idx < position; idx++) {
                    writeStringOrNull(blockBuilder, firstValue);
                }
                writeStringOrNull(blockBuilder, deleteFilePath);
                allSameValues = false;
            }
            else if (!allSameValues) {
                writeStringOrNull(blockBuilder, deleteFilePath);
            }
        }

        Block block;
        if (blockBuilder != null) {
            block = blockBuilder.build();
        }
        else {
            Slice slice = firstValue.map(Slices::utf8Slice).orElse(null);
            block = RunLengthEncodedBlock.create(VARCHAR, slice, positionCount);
        }

        return page.replaceColumn(deleteFilePathDelegateColumnId, block);
    }

    private void writeStringOrNull(BlockBuilder blockBuilder, Optional<String> toWrite)
    {
        if (toWrite.isPresent()) {
            VARCHAR.writeString(blockBuilder, toWrite.get());
        }
        else {
            blockBuilder.appendNull();
        }
    }

    private Optional<String> getDeleteFilePath(Page page, int position, List<Pair<DeleteFilter, RowPredicate>> filterPredicates)
    {
        for (Pair<DeleteFilter, RowPredicate> pair : filterPredicates) {
            boolean deleted = !pair.second().test(page, position);
            if (deleted) {
                String path = pair.first().getDeleteFilePath().orElse(null);
                if (path != null) {
                    return Optional.of(path);
                }
            }
        }
        return Optional.empty();
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
}
