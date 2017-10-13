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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.spi.IndexPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AppendingPageSource
        implements IndexPageSource
{
    private final IndexPageSource delegate;
    private final List<Object> appendedValues;
    private final List<Type> appendedTypes;

    AppendingPageSource(IndexPageSource delegate, List<Type> types, List<Object> values)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.appendedTypes = requireNonNull(types, "types is null");
        this.appendedValues = requireNonNull(values, "value is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return ImmutableList.<Type>builder()
                .addAll(delegate.getColumnTypes())
                .addAll(appendedTypes)
                .build();
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
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
        if (isFinished()) {
            return null;
        }
        Page page = getNextPage();
        Block[] blocks = new Block[page.getChannelCount() + appendedValues.size()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            blocks[i] = page.getBlock(i);
        }

        for (int i = 0; i < appendedValues.size(); i++) {
            Type type = appendedTypes.get(i);
            BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
            Object value = appendedValues.get(i);
            long positions = page.getPositionCount();
            Class<?> javaType = type.getJavaType();
            if (value == null) {
                for (int j = 0; j < positions; i++) {
                    blockBuilder.appendNull();
                }
            }
            else if (javaType == long.class) {
                for (int j = 0; j < positions; i++) {
                    blockBuilder.writeLong((Long) value);
                }
            }
            blocks[page.getChannelCount() + i] = blockBuilder.build();
        }
        return new Page(blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
