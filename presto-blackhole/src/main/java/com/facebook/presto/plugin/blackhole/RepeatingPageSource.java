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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

public class RepeatingPageSource
        implements ConnectorPageSource
{
    private final Page page;
    private final int pagesCount;
    private int currentPage = 0;

    public RepeatingPageSource(Page page, int pagesCount)
    {
        this.pagesCount = pagesCount;
        this.page = page;
    }

    @Override
    public long getTotalBytes()
    {
        return page.getSizeInBytes() * pagesCount;
    }

    @Override
    public long getCompletedBytes()
    {
        return page.getSizeInBytes() * currentPage;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return currentPage >= pagesCount;
    }

    @Override
    public Page getNextPage()
    {
        currentPage++;
        return page;
    }

    @Override
    public void close()
    {
    }
}
