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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper page source that replaces NULL values with default values for columns that have them.
 * This is used to support Iceberg v3 default values feature where columns added with ALTER TABLE
 * have default values that should be returned for old data files that don't contain the column.
 */
public class IcebergDefaultValuePageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<IcebergColumnHandle> columns;
    private final Map<Integer, DefaultValueInfo> defaultValueInfos;

    public IcebergDefaultValuePageSource(
            ConnectorPageSource delegate,
            List<IcebergColumnHandle> columns,
            Map<Integer, Object> defaultValues)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columns = requireNonNull(columns, "columns is null");
        requireNonNull(defaultValues, "defaultValues is null");
        this.defaultValueInfos = columns.stream()
                .filter(col -> defaultValues.containsKey(col.getId()))
                .collect(toImmutableMap(
                        IcebergColumnHandle::getId,
                        col -> new DefaultValueInfo(col.getType(), defaultValues.get(col.getId()))));
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
        Page page = delegate.getNextPage();
        if (page == null || defaultValueInfos.isEmpty()) {
            return page;
        }
        // Process each block and replace NULLs with defaults if needed
        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            blocks[channel] = block;
            // Get the column handle for this channel
            if (channel < columns.size()) {
                IcebergColumnHandle column = columns.get(channel);
                DefaultValueInfo defaultInfo = defaultValueInfos.get(column.getId());
                if (defaultInfo != null) {
                    blocks[channel] = replaceNullsWithDefault(block, defaultInfo);
                }
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private Block replaceNullsWithDefault(Block block, DefaultValueInfo defaultInfo)
    {
        int positionCount = block.getPositionCount();
        int nullCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                nullCount++;
            }
        }
        // All NULL - use pre-computed RLE block (common for old files)
        if (nullCount == positionCount) {
            return new RunLengthEncodedBlock(defaultInfo.getBlock(), positionCount);
        }
        // No NULLs - return original block unchanged (common for new files)
        if (nullCount == 0) {
            return block;
        }
        // Mixed case - build new block with selective replacement
        Type type = defaultInfo.getType();
        Object defaultValue = defaultInfo.getValue();
        BlockBuilder builder = type.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                writeNativeValue(type, builder, defaultValue);
            }
            else {
                type.appendTo(block, position, builder);
            }
        }
        return builder.build();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        long defaultValuesMemory = defaultValueInfos.values().stream()
                .mapToLong(DefaultValueInfo::getRetainedSizeInBytes)
                .sum();
        return delegate.getSystemMemoryUsage() + defaultValuesMemory;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    /**
     * Encapsulates default value information for a column.
     */
    private static class DefaultValueInfo
    {
        private final Type type;
        private final Object value;
        private Block block;

        public DefaultValueInfo(Type type, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.value = value;
        }

        public Type getType()
        {
            return type;
        }

        public Object getValue()
        {
            return value;
        }

        public synchronized Block getBlock()
        {
            if (block == null) {
                block = nativeValueToBlock(type, value);
            }
            return block;
        }

        public long getRetainedSizeInBytes()
        {
            return block == null ? 0 : block.getRetainedSizeInBytes();
        }
    }
}
