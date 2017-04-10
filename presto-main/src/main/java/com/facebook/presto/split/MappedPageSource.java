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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class MappedPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final int[] delegateFieldIndex;

    public MappedPageSource(ConnectorPageSource delegate, List<Integer> delegateFieldIndex)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delegateFieldIndex = Ints.toArray(requireNonNull(delegateFieldIndex, "delegateFieldIndex is null"));
    }

    @Override
    public long getTotalBytes()
    {
        return delegate.getTotalBytes();
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
        Page nextPage = delegate.getNextPage();
        if (nextPage == null) {
            return null;
        }
        Block[] blocks = Arrays.stream(delegateFieldIndex)
                .mapToObj(nextPage::getBlock)
                .toArray(Block[]::new);
        return new Page(nextPage.getPositionCount(), blocks);
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
