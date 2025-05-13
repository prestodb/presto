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
package com.facebook.presto.split;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.UpdatablePageSource;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class EmptySplitPageSource
        implements UpdatablePageSource
{
    @Override
    public void deleteRows(Block rowIds)
    {
        throw new UnsupportedOperationException("deleteRows called on EmptySplitPageSource");
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        throw new UnsupportedOperationException("updateRows called on EmptySplitPageSource");
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Override
    public Page getNextPage()
    {
        return null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
