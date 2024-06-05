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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.RowIDCoercer;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcBatchPageSource
        implements ConnectorPageSource
{
    private final OrcBatchRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;
    private final boolean[] rowIDColumnIndexes;

    private int batchId;
    private long completedPositions;
    private boolean closed;

    private final OrcAggregatedMemoryContext systemMemoryContext;

    private final FileFormatDataSourceStats stats;

    private final RuntimeStats runtimeStats;

    private final boolean[] isRowNumberList;

    private final RowIDCoercer coercer;

    /**
     * @param columns an ordered list of the fields to read
     * @param isRowNumberList list of indices of columns. If true, then the column then the column
     *     at the same position in {@code columns} is a row number. If false, it isn't.
     *     This should have the same length as {@code columns}.
     * #throws IllegalArgumentException if columns and isRowNumberList do not have the same size
     */
    // TODO(elharo) HiveColumnHandle should know whether it's a row number or not. Alternatively,
    //  define a class that includes both a column handle and the row number boolean.
    public OrcBatchPageSource(
            OrcBatchRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            OrcAggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats,
            RuntimeStats runtimeStats,
            // TODO avoid conversion; just pass a boolean array here
            List<Boolean> isRowNumberList,
            byte[] rowIDPartitionComponent,
            String rowGroupId)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");

        int numColumns = requireNonNull(columns, "columns is null").size();

        this.stats = requireNonNull(stats, "stats is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
        requireNonNull(isRowNumberList, "isRowNumberList is null");
        checkArgument(isRowNumberList.size() == numColumns, "row number list size %s does not match columns size %s", isRowNumberList.size(), columns.size());
        this.isRowNumberList = Booleans.toArray(isRowNumberList);
        this.coercer = new RowIDCoercer(rowIDPartitionComponent, rowGroupId);

        this.constantBlocks = new Block[numColumns];
        this.hiveColumnIndexes = new int[numColumns];
        this.rowIDColumnIndexes = new boolean[numColumns];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            checkState(column.getColumnType() == REGULAR, "column type of %s must be REGULAR but was %s", column.getName(), column.getColumnType().name());

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();
            rowIDColumnIndexes[columnIndex] = HiveColumnHandle.isRowIdColumnHandle(column);

            if (!recordReader.isColumnPresent(column.getHiveColumnIndex())) {
                constantBlocks[columnIndex] = RunLengthEncodedBlock.create(type, null, MAX_BATCH_SIZE);
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();

        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            int batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }

            completedPositions += batchSize;

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (isRowPositionColumn(fieldId)) {
                    blocks[fieldId] = getRowPosColumnBlock(recordReader.getFilePosition(), batchSize);
                }
                else if (isRowIDColumn(fieldId)) {
                    Block rowNumbers = getRowPosColumnBlock(recordReader.getFilePosition(), batchSize);
                    Block rowIDs = coercer.apply(rowNumbers);
                    blocks[fieldId] = rowIDs;
                }
                else if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    blocks[fieldId] = new LazyBlock(batchSize, new OrcBlockLoader(hiveColumnIndexes[fieldId]));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (OrcCorruptionException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnNames)
                .add("types", types)
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
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

    private boolean isRowPositionColumn(int column)
    {
        return isRowNumberList[column];
    }

    private boolean isRowIDColumn(int column)
    {
        return this.rowIDColumnIndexes[column];
    }

    // TODO verify these are row numbers and rename?
    private static Block getRowPosColumnBlock(long baseIndex, int size)
    {
        long[] rowPositions = new long[size];
        for (int position = 0; position < size; position++) {
            rowPositions[position] = baseIndex + position;
        }
        return new LongArrayBlock(size, Optional.empty(), rowPositions);
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private boolean loaded;

        public OrcBlockLoader(int columnIndex)
        {
            this.columnIndex = columnIndex;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = recordReader.readBlock(columnIndex);
                lazyBlock.setBlock(block);
            }
            catch (OrcCorruptionException e) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            catch (IOException | RuntimeException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
            }

            loaded = true;
        }
    }
}
