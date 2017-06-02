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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.SubColumnBlock;
import com.facebook.presto.spi.block.SubColumnBlock.ColumnHandleReference;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class OrcPageSource
        implements ConnectorPageSource
{
    private static final int NULL_ENTRY_SIZE = 0;
    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    private final Block[] constantBlocks;
    private final HiveColumnHandle[] hiveColumnIndexes;

    private int batchId;
    private boolean closed;

    private final AggregatedMemoryContext systemMemoryContext;
    private final TypeManager typeManager;

    private final FileFormatDataSourceStats stats;

    public OrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");

        int size = requireNonNull(columns, "columns is null").size();

        this.stats = requireNonNull(stats, "stats is null");

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new HiveColumnHandle[size];
        this.typeManager = typeManager;

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            checkState(column.isVariableDataColumn(), "column type must be regular");

            Type type = typeManager.getType(column.getTypeSignature());

            hiveColumnIndexes[columnIndex] = column;

            if (!recordReader.isColumnPresent(column.getHiveColumnIndex())) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_BATCH_SIZE, NULL_ENTRY_SIZE);
                for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                    blockBuilder.appendNull();
                }
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }

        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public long getTotalBytes()
    {
        return recordReader.getSplitLength();
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
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

            Map<Integer, LazyBlock> cached = new HashMap<>();
            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    SubColumnReference reference = new SubColumnReference(typeManager, hiveColumnIndexes[fieldId]);
                    List<ColumnHandleReference> hirarchies = reference.getHirarchies();
                    ColumnHandleReference parent = hirarchies.size() == 1 ? reference : reference.getHirarchies().get(0);

                    LazyBlock block;
                    if (cached.containsKey(parent.ordinal())) {
                        block = cached.get(parent.ordinal());
                    }
                    else {
                        block = new LazyBlock(batchSize, new OrcBlockLoader(parent.ordinal(), parent.getType(), stats));
                        cached.put(parent.ordinal(), block);
                    }

                    blocks[fieldId] = hirarchies.size() == 1 ? block : new SubColumnBlock(block.getBlock(), reference);
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
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
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hiveColumnIndexes", hiveColumnIndexes)
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

    private final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private final Type type;
        private final FileFormatDataSourceStats stats;
        private boolean loaded;

        public OrcBlockLoader(int columnIndex, Type type, FileFormatDataSourceStats stats)
        {
            this.columnIndex = columnIndex;
            this.type = requireNonNull(type, "type is null");
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = recordReader.readBlock(type, columnIndex);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                if (e instanceof OrcCorruptionException) {
                    throw new PrestoException(HIVE_BAD_DATA, e);
                }
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }

            stats.addLoadedBlockSize(lazyBlock.getSizeInBytes());

            loaded = true;
        }
    }
}
