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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PageStore
{
    private final PageBuffer pageBuffer;
    private final PageBuilder pageBuilder;

    public PageStore(PageBuffer pageBuffer, List<Type> columnTypes)
    {
        this.pageBuffer = requireNonNull(pageBuffer, "pageBuffer is null");
        this.pageBuilder = new PageBuilder(columnTypes);
    }

    public long getUsedMemoryBytes()
    {
        return pageBuilder.getSizeInBytes() + pageBuffer.getUsedMemoryBytes();
    }

    public PageBuffer getPageBuffer()
    {
        return pageBuffer;
    }

    public void appendPosition(Page page, int position)
    {
        pageBuilder.declarePosition();
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
            pageBuilder.getType(channel).appendTo(block, position, blockBuilder);
        }

        if (pageBuilder.isFull()) {
            flushToPageBuffer();
        }
    }

    public void flushToPageBuffer()
    {
        if (!pageBuilder.isEmpty()) {
            pageBuffer.add(pageBuilder.build());
            pageBuilder.reset();
        }
    }
}
