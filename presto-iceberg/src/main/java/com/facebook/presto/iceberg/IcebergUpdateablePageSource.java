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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

/**
 * Generates pages for an iceberg table while applying delete filters to rows
 * _and_ by modifying the output format of the output pages for update
 * operators if required.
 */
public class IcebergUpdateablePageSource
        implements UpdatablePageSource
{
    private final ConnectorPageSource delegate;
    private final Supplier<IcebergDeletePageSink> deleteSinkSupplier;
    private IcebergDeletePageSink positionDeleteSink;
    private final Supplier<Optional<RowPredicate>> deletePredicate;

    private final List<IcebergColumnHandle> columns;
    private final Optional<IcebergColumnHandle> updateRowIdColumn;
    /**
     * Columns actually updated in the query
     */
    private final List<IcebergColumnHandle> updatedColumns;
    private final Schema tableSchema;
    private final Supplier<IcebergPageSink> updatedRowPageSinkSupplier;
    private IcebergPageSink updatedRowPageSink;
    // An array with one element per field in the $row_id column. The value in the array points to the
    // channel where the data can be read from within the input page
    private final int[] updateRowIdChildColumnIndexes;
    // The $row_id's index in 'expectedColumns', or -1 if there isn't one
    private final int updateRowIdColumnIndex;
    // Maps the Iceberg field ids of unmodified columns to their indexes in updateRowIdChildColumnIndexes
    private final Map<Integer, Integer> icebergIdToRowIdColumnIndex = new HashMap<>();
    // Maps the Iceberg field ids of modified columns to their indexes in the updatedColumns columnValueAndRowIdChannels array
    private final Map<Integer, Integer> icebergIdToUpdatedColumnIndex = new HashMap<>();
    private final int[] outputColumnToDelegateMapping;

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
            Supplier<IcebergPageSink> updatedRowPageSinkSupplier,
            // the columns that this page source is supposed to update
            List<IcebergColumnHandle> updatedColumns,
            Optional<IcebergColumnHandle> updateRowIdColumn)
    {
        requireNonNull(partitionKeys, "partitionKeys is null");
        this.tableSchema = requireNonNull(tableSchema, "tableSchema is null");
        this.columns = requireNonNull(outputColumns, "columns is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        // information for deletes
        this.deleteSinkSupplier = deleteSinkSupplier;
        this.deletePredicate = requireNonNull(deletePredicate, "deletePredicate is null");
        // information for updates
        this.updatedRowPageSinkSupplier = requireNonNull(updatedRowPageSinkSupplier, "updatedRowPageSinkSupplier is null");
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
        this.updateRowIdColumn = requireNonNull(updateRowIdColumn, "updateRowIdColumn is null");
        this.outputColumnToDelegateMapping = new int[columns.size()];
        this.updateRowIdColumnIndex = updateRowIdColumn.map(columns::indexOf).orElse(-1);
        this.updateRowIdChildColumnIndexes = updateRowIdColumn
                .map(column -> new int[column.getColumnIdentity().getChildren().size()])
                .orElse(new int[0]);
        Map<Integer, Integer> fieldIdToIndex = IntStream.range(0, delegateColumns.size())
                .boxed()
                .collect(toImmutableMap(
                        idx -> delegateColumns.get(idx).getId(),
                        identity()));
        updateRowIdColumn.ifPresent(column -> {
            List<ColumnIdentity> rowIdFields = column.getColumnIdentity().getChildren();
            for (int i = 0; i < rowIdFields.size(); i++) {
                int fieldId = rowIdFields.get(i).getId();
                updateRowIdChildColumnIndexes[i] = requireNonNull(fieldIdToIndex.get(fieldId), () -> format("Column %s not found in requiredColumns", fieldId));
                icebergIdToRowIdColumnIndex.put(fieldId, i);
            }
        });

        if (!updatedColumns.isEmpty()) {
            for (int columnIndex = 0; columnIndex < updatedColumns.size(); columnIndex++) {
                IcebergColumnHandle updatedColumn = updatedColumns.get(columnIndex);
                icebergIdToUpdatedColumnIndex.put(updatedColumn.getId(), columnIndex);
            }
        }
        for (int i = 0; i < outputColumnToDelegateMapping.length; i++) {
            if (outputColumns.get(i).isUpdateRowIdColumn()) {
                continue;
            }

            if (!fieldIdToIndex.containsKey(outputColumns.get(i).getId())) {
                throw new IllegalArgumentException(format("column %s does not exist in delegate column map", outputColumns.get(i)));
            }
            else {
                outputColumnToDelegateMapping[i] = fieldIdToIndex.get(outputColumns.get(i).getId());
            }
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
        // strategy
        // 1. retrieve rows from delegate
        // 2. apply delete filter to page
        // 4. transform page for update (turn flat blocks into nested update row)
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
            }
            else {
                Block[] fullPage = new Block[columns.size()];
                for (int channel = 0; channel < columns.size(); channel++) {
                    fullPage[channel] = dataPage.getBlock(outputColumnToDelegateMapping[channel]);
                }
                dataPage = new Page(dataPage.getPositionCount(), fullPage);
            }

            return dataPage;
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

        Block[] fullPage = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            if (channel == updateRowIdColumnIndex) {
                fullPage[channel] = RowBlock.fromFieldBlocks(page.getPositionCount(), Optional.empty(), rowIdFields);
                continue;
            }
            fullPage[channel] = page.getBlock(outputColumnToDelegateMapping[channel]);
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

    private static Map<Integer, Integer> mapFieldIdsToIndex(Stream<Integer> columns)
    {
        ImmutableMap.Builder<Integer, Integer> fieldIdsToIndex = ImmutableMap.builder();
        int idx = 0;
        Iterator<Integer> iter = columns.iterator();
        while (iter.hasNext()) {
            Integer columnId = iter.next();
            fieldIdsToIndex.put(columnId, idx);
            idx++;
        }
        return fieldIdsToIndex.buildOrThrow();
    }
}
