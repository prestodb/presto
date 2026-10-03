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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a Parquet page source and replaces the all-null block a column absent from the file
 * produces with its {@code initial_default} value (spec &sect;4.4), so that a file written before
 * a column existed still returns that column's default instead of {@code NULL}. Never touches a
 * column the file has (its real {@code NULL}s are left alone). Modeled on {@code
 * IcebergDefaultValuePageSource}.
 */
class DefaultValuePageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<DuckLakeColumnHandle> columns;
    private final Map<Long, DefaultValueInfo> defaultValueInfos;

    DefaultValuePageSource(ConnectorPageSource delegate, List<DuckLakeColumnHandle> columns, Map<Long, Object> defaultValues)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columns = requireNonNull(columns, "columns is null");
        requireNonNull(defaultValues, "defaultValues is null");
        this.defaultValueInfos = columns.stream()
                .filter(column -> defaultValues.containsKey(column.getId()))
                .collect(toImmutableMap(
                        DuckLakeColumnHandle::getId,
                        column -> new DefaultValueInfo(column.getType(), defaultValues.get(column.getId()))));
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
        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            blocks[channel] = block;
            if (channel < columns.size()) {
                DefaultValueInfo defaultInfo = defaultValueInfos.get(columns.get(channel).getId());
                if (defaultInfo != null) {
                    blocks[channel] = replaceNullsWithDefault(block, defaultInfo);
                }
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static Block replaceNullsWithDefault(Block block, DefaultValueInfo defaultInfo)
    {
        int positionCount = block.getPositionCount();
        int nullCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                nullCount++;
            }
        }
        if (nullCount == positionCount) {
            // The common case: the whole column is absent from this file.
            return new RunLengthEncodedBlock(defaultInfo.getBlock(), positionCount);
        }
        if (nullCount == 0) {
            return block;
        }
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

    private static class DefaultValueInfo
    {
        private final Type type;
        private final Object value;
        private Block block;

        DefaultValueInfo(Type type, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.value = value;
        }

        Type getType()
        {
            return type;
        }

        Object getValue()
        {
            return value;
        }

        synchronized Block getBlock()
        {
            if (block == null) {
                block = nativeValueToBlock(type, value);
            }
            return block;
        }

        long getRetainedSizeInBytes()
        {
            return block == null ? 0 : block.getRetainedSizeInBytes();
        }
    }
}
