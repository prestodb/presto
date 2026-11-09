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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.UpdatablePageSource;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Wraps a Lance scan page source to support row-level DELETE and UPDATE using the
 * merge-on-read pattern. The wrapped scan exposes the {@code _rowaddr} column; deleted rows
 * are accumulated per fragment, and for UPDATE the rewritten rows are buffered into new
 * fragments. {@link #finish()} serializes both for the coordinator to commit atomically.
 */
public class LanceUpdatablePageSource
        implements UpdatablePageSource
{
    private final ConnectorPageSource delegate;
    private final long readVersion;
    private final JsonCodec<LanceMergeCommitData> commitDataCodec;
    private final Optional<UpdateContext> updateContext;

    private final Map<Integer, List<Integer>> deletionsByFragment = new LinkedHashMap<>();
    private final List<Page> newRowPages = new ArrayList<>();
    private long deletedRowCount;

    public LanceUpdatablePageSource(
            ConnectorPageSource delegate,
            long readVersion,
            JsonCodec<LanceMergeCommitData> commitDataCodec)
    {
        this(delegate, readVersion, commitDataCodec, Optional.empty());
    }

    public LanceUpdatablePageSource(
            ConnectorPageSource delegate,
            long readVersion,
            JsonCodec<LanceMergeCommitData> commitDataCodec,
            Optional<UpdateContext> updateContext)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.readVersion = readVersion;
        this.commitDataCodec = requireNonNull(commitDataCodec, "commitDataCodec is null");
        this.updateContext = requireNonNull(updateContext, "updateContext is null");
    }

    @Override
    public Page getNextPage()
    {
        Page dataPage = delegate.getNextPage();
        if (dataPage == null || !updateContext.isPresent()) {
            return dataPage;
        }
        // Re-assemble the $row_id struct column expected on the output from the flattened
        // columns produced by the delegate scan.
        UpdateContext context = updateContext.get();
        Block[] blocks = new Block[context.outputColumns.size()];
        for (int channel = 0; channel < blocks.length; channel++) {
            if (channel == context.rowIdOutputChannel) {
                Block[] fieldBlocks = new Block[context.rowIdFieldToDelegate.length];
                for (int field = 0; field < fieldBlocks.length; field++) {
                    fieldBlocks[field] = dataPage.getBlock(context.rowIdFieldToDelegate[field]);
                }
                blocks[channel] = RowBlock.fromFieldBlocks(dataPage.getPositionCount(), Optional.empty(), fieldBlocks);
            }
            else {
                blocks[channel] = dataPage.getBlock(context.outputToDelegate[channel]);
            }
        }
        return new Page(dataPage.getPositionCount(), blocks);
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        for (int position = 0; position < rowIds.getPositionCount(); position++) {
            if (!rowIds.isNull(position)) {
                markRowDeleted(BIGINT.getLong(rowIds, position));
            }
        }
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        UpdateContext context = updateContext.orElseThrow(() ->
                new PrestoException(LanceErrorCode.LANCE_ERROR, "Update context not initialized for UPDATE"));

        int rowIdChannel = columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1);
        List<Integer> valueChannels = columnValueAndRowIdChannels.subList(0, columnValueAndRowIdChannels.size() - 1);
        ColumnarRow rowId = toColumnarRow(page.getBlock(rowIdChannel));

        // Field 0 of the row id struct is the row address; mark those rows for deletion.
        Block rowAddressBlock = rowId.getField(0);
        for (int position = 0; position < rowAddressBlock.getPositionCount(); position++) {
            if (!rowAddressBlock.isNull(position)) {
                markRowDeleted(BIGINT.getLong(rowAddressBlock, position));
            }
        }

        // Map each updated column name to the page channel carrying its new value.
        Map<String, Integer> updatedNameToChannel = new LinkedHashMap<>();
        for (int i = 0; i < context.updatedColumnNames.size(); i++) {
            updatedNameToChannel.put(context.updatedColumnNames.get(i), valueChannels.get(i));
        }

        // Assemble full rewritten rows in table column order: updated columns come from the
        // page, unmodified columns come from the row id struct.
        Block[] fullRow = new Block[context.tableColumnNames.size()];
        for (int target = 0; target < fullRow.length; target++) {
            String name = context.tableColumnNames.get(target);
            Integer updatedChannel = updatedNameToChannel.get(name);
            if (updatedChannel != null) {
                fullRow[target] = page.getBlock(updatedChannel);
            }
            else {
                fullRow[target] = rowId.getField(context.unmodifiedNameToRowIdField.get(name));
            }
        }
        newRowPages.add(new Page(page.getPositionCount(), fullRow));
    }

    private void markRowDeleted(long rowAddress)
    {
        int fragmentId = RowAddress.fragmentId(rowAddress);
        int rowIndex = RowAddress.rowIndex(rowAddress);
        deletionsByFragment.computeIfAbsent(fragmentId, key -> new ArrayList<>()).add(rowIndex);
        deletedRowCount++;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<FragmentDeletion> deletions = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> entry : deletionsByFragment.entrySet()) {
            deletions.add(new FragmentDeletion(entry.getKey(), entry.getValue()));
        }

        String newFragmentsJson = newRowPages.isEmpty() ? "[]" : writeNewFragments();

        LanceMergeCommitData commitData = new LanceMergeCommitData(
                deletions,
                newFragmentsJson,
                readVersion,
                0L,
                deletedRowCount);
        Slice slice = Slices.wrappedBuffer(commitDataCodec.toJsonBytes(commitData));
        return completedFuture(ImmutableList.of(slice));
    }

    private String writeNewFragments()
    {
        UpdateContext context = updateContext.orElseThrow(() ->
                new PrestoException(LanceErrorCode.LANCE_ERROR, "Update context not initialized for UPDATE"));
        try (BufferAllocator allocator = context.allocator.newChildAllocator("lance-update-write", 0, Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(context.arrowSchema, allocator)) {
            long totalRowsLong = newRowPages.stream().mapToLong(Page::getPositionCount).sum();
            if (totalRowsLong > Integer.MAX_VALUE) {
                throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Updated row count exceeds maximum: " + totalRowsLong);
            }
            root.allocateNew();
            int offset = 0;
            for (Page page : newRowPages) {
                int pageRows = page.getPositionCount();
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    LancePageToArrowConverter.writeBlockToVectorAtOffset(
                            page.getBlock(channel),
                            root.getVector(channel),
                            context.columnTypes.get(channel),
                            pageRows,
                            offset);
                }
                offset += pageRows;
            }
            root.setRowCount((int) totalRowsLong);

            List<FragmentMetadata> fragments = Fragment.create(
                    context.datasetUri, allocator, root, new WriteParams.Builder().build());
            return LanceFragmentData.serializeFragments(fragments);
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
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
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

    /**
     * Precomputed mappings the page source needs to re-assemble the {@code $row_id} struct on
     * output and to write rewritten rows as new fragments during UPDATE.
     */
    public static class UpdateContext
    {
        private final List<LanceColumnHandle> outputColumns;
        private final int rowIdOutputChannel;
        private final int[] outputToDelegate;
        private final int[] rowIdFieldToDelegate;
        private final Map<String, Integer> unmodifiedNameToRowIdField;
        private final List<String> tableColumnNames;
        private final List<String> updatedColumnNames;
        private final List<Type> columnTypes;
        private final Schema arrowSchema;
        private final BufferAllocator allocator;
        private final String datasetUri;

        public UpdateContext(
                List<LanceColumnHandle> outputColumns,
                List<LanceColumnHandle> delegateColumns,
                LanceColumnHandle rowIdColumn,
                List<LanceColumnHandle> tableColumns,
                List<LanceColumnHandle> updatedColumns,
                Schema arrowSchema,
                BufferAllocator allocator,
                String datasetUri)
        {
            this.outputColumns = ImmutableList.copyOf(outputColumns);
            this.arrowSchema = requireNonNull(arrowSchema, "arrowSchema is null");
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.datasetUri = requireNonNull(datasetUri, "datasetUri is null");

            Map<String, Integer> delegateNameToIndex = new LinkedHashMap<>();
            for (int i = 0; i < delegateColumns.size(); i++) {
                delegateNameToIndex.put(delegateColumns.get(i).getColumnName(), i);
            }

            this.rowIdOutputChannel = outputColumns.indexOf(rowIdColumn);
            this.outputToDelegate = new int[outputColumns.size()];
            for (int i = 0; i < outputColumns.size(); i++) {
                if (i == rowIdOutputChannel) {
                    continue;
                }
                outputToDelegate[i] = delegateNameToIndex.get(outputColumns.get(i).getColumnName());
            }

            List<String> rowIdFieldNames = rowTypeFieldNames(rowIdColumn.getColumnType());
            this.rowIdFieldToDelegate = new int[rowIdFieldNames.size()];
            this.unmodifiedNameToRowIdField = new LinkedHashMap<>();
            for (int field = 0; field < rowIdFieldNames.size(); field++) {
                String name = rowIdFieldNames.get(field);
                rowIdFieldToDelegate[field] = delegateNameToIndex.get(name);
                if (field > 0) {
                    unmodifiedNameToRowIdField.put(name, field);
                }
            }

            ImmutableList.Builder<String> tableNames = ImmutableList.builder();
            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (LanceColumnHandle column : tableColumns) {
                tableNames.add(column.getColumnName());
                types.add(column.getColumnType());
            }
            this.tableColumnNames = tableNames.build();
            this.columnTypes = types.build();

            ImmutableList.Builder<String> updatedNames = ImmutableList.builder();
            for (LanceColumnHandle column : updatedColumns) {
                updatedNames.add(column.getColumnName());
            }
            this.updatedColumnNames = updatedNames.build();
        }

        private static List<String> rowTypeFieldNames(Type rowType)
        {
            ImmutableList.Builder<String> names = ImmutableList.builder();
            for (RowType.Field field : ((RowType) rowType).getFields()) {
                names.add(field.getName().orElseThrow(() ->
                        new PrestoException(LanceErrorCode.LANCE_ERROR, "Row id struct field is missing a name")));
            }
            return names.build();
        }
    }
}
