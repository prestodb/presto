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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class PageBuffer
{
    private final long maxMemoryBytes;
    private final List<Page> pages = new ArrayList<>();

    private long usedMemoryBytes;
    private long rowCount;

    public PageBuffer(long maxMemoryBytes)
    {
        checkArgument(maxMemoryBytes > 0, "maxMemoryBytes must be positive");
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public void add(Page page)
    {
        checkNotNull(page, "page is null");
        pages.add(page);
        usedMemoryBytes += page.getSizeInBytes();
        rowCount += page.getPositionCount();
    }

    public void reset()
    {
        pages.clear();
        rowCount = 0;
        usedMemoryBytes = 0;
    }

    public boolean isFull()
    {
        return usedMemoryBytes >= maxMemoryBytes;
    }

    public List<Page> getPages()
    {
        return ImmutableList.copyOf(pages);
    }

    public long getRowCount()
    {
        return rowCount;
    }
}
