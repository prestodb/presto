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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.raptor.storage.DeltaShardLoader.RowsToKeepResult;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class OrcPageSource
        implements ConnectorPageSource
{
    public static final int NULL_COLUMN = -1;
    public static final int ROWID_COLUMN = -2;
    public static final int SHARD_UUID_COLUMN = -3;
    public static final int BUCKET_NUMBER_COLUMN = -4;

    private final OrcBatchRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    // for shard with existing delta
    private final DeltaShardLoader deltaShardLoader;

    private final List<Long> columnIds;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] columnIndexes;

    private final OrcAggregatedMemoryContext systemMemoryContext;

    private int batchId;
    private long completedPositions;
    private boolean closed;

    public OrcPageSource(
            OrcBatchRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<Long> columnIds,
            List<Type> columnTypes,
            List<Integer> columnIndexes,
            UUID shardUuid,
            OptionalInt bucketNumber,
            OrcAggregatedMemoryContext systemMemoryContext,
            DeltaShardLoader deltaShardLoader)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.deltaShardLoader = requireNonNull(deltaShardLoader, "Optional<deltaShardUuid> is null");

        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(columnIds.size() == columnIndexes.size(), "ids and indexes mismatch");
        int size = columnIds.size();

        this.columnIds = ImmutableList.copyOf(columnIds);
        this.types = ImmutableList.copyOf(columnTypes);

        this.constantBlocks = new Block[size];
        this.columnIndexes = new int[size];

        requireNonNull(shardUuid, "shardUuid is null");

        for (int i = 0; i < size; i++) {
            this.columnIndexes[i] = columnIndexes.get(i);
            if (this.columnIndexes[i] == NULL_COLUMN) {
                constantBlocks[i] = buildSingleValueBlock(columnTypes.get(i), null);
            }
            else if (this.columnIndexes[i] == SHARD_UUID_COLUMN) {
                constantBlocks[i] = buildSingleValueBlock(columnTypes.get(i), utf8Slice(shardUuid.toString()));
            }
            else if (this.columnIndexes[i] == BUCKET_NUMBER_COLUMN) {
                if (bucketNumber.isPresent()) {
                    constantBlocks[i] = buildSingleValueBlock(columnTypes.get(i), (long) bucketNumber.getAsInt());
                }
                else {
                    constantBlocks[i] = buildSingleValueBlock(columnTypes.get(i), null);
                }
            }
        }

        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
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

            long filePosition = recordReader.getFilePosition();

            // for every page, will generate its rowsToKeep
            RowsToKeepResult rowsToKeep = deltaShardLoader.getRowsToKeep(batchSize, filePosition);

            Block[] blocks = new Block[columnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, rowsToKeep.keepAll() ? batchSize : rowsToKeep.size());
                }
                else if (columnIndexes[fieldId] == ROWID_COLUMN) {
                    blocks[fieldId] = buildSequenceBlock(filePosition, batchSize, rowsToKeep);
                }
                else {
                    blocks[fieldId] = new LazyBlock(batchSize, new OrcBlockLoader(columnIndexes[fieldId], rowsToKeep));
                }
            }

            return new Page(rowsToKeep.keepAll() ? batchSize : rowsToKeep.size(), blocks);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        closed = true;

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnIds)
                .add("types", types)
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    private void closeWithSuppression(Throwable throwable)
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

    private static Block buildSequenceBlock(long start, int count, RowsToKeepResult rowsToKeep)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            if (rowsToKeep.keepAll() || rowsToKeep.getRowsToKeep().contains(i)) {
                BIGINT.writeLong(builder, start + i);
            }
        }
        return builder.build();
    }

    private static Block buildSingleValueBlock(Type type, Object value)
    {
        Block block = nativeValueToBlock(type, value);
        return new RunLengthEncodedBlock(block, MAX_BATCH_SIZE);
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private final RowsToKeepResult rowsToKeep;
        private boolean loaded;

        public OrcBlockLoader(int columnIndex, RowsToKeepResult rowsToKeep)
        {
            this.columnIndex = columnIndex;
            this.rowsToKeep = rowsToKeep;
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
                if (rowsToKeep.keepAll()) {
                    lazyBlock.setBlock(block);
                }
                else {
                    lazyBlock.setBlock(block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size()));
                }
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, e);
            }

            loaded = true;
        }
    }
}
