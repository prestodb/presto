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
package com.facebook.presto.druid;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.druid.segment.DruidSegmentReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DruidSegmentPageSource
        implements ConnectorPageSource
{
    private final DataInputSource dataInputSource;
    private final List<ColumnHandle> columns;
    private final DruidSegmentReader segmentReader;

    private int batchId;
    private boolean closed;
    private long completedBytes;
    private long completedPositions;

    public DruidSegmentPageSource(
            DataInputSource dataInputSource,
            List<ColumnHandle> columns,
            DruidSegmentReader segmentReader)
    {
        this.dataInputSource = requireNonNull(dataInputSource, "dataInputSource is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.segmentReader = requireNonNull(segmentReader, "segmentReader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataInputSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        batchId++;
        int batchSize = segmentReader.nextBatch();
        if (batchSize <= 0) {
            close();
            return null;
        }
        Block[] blocks = new Block[columns.size()];
        for (int i = 0; i < blocks.length; i++) {
            DruidColumnHandle columnHandle = (DruidColumnHandle) columns.get(i);
            blocks[i] = new LazyBlock(batchSize, new SegmentBlockLoader(columnHandle.getColumnType(), columnHandle.getColumnName()));
        }
        Page page = new Page(batchSize, blocks);
        completedBytes += page.getSizeInBytes();
        completedPositions += page.getPositionCount();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
        // TODO: close all column reader and value selectors
    }

    private final class SegmentBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final Type type;
        private final String name;
        private boolean loaded;

        public SegmentBlockLoader(Type type, String name)
        {
            this.type = requireNonNull(type, "type is null");
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            Block block = segmentReader.readBlock(type, name);
            lazyBlock.setBlock(block);
            loaded = true;
        }
    }
}
