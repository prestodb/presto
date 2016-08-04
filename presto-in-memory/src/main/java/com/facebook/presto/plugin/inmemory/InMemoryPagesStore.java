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
package com.facebook.presto.plugin.inmemory;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class InMemoryPagesStore
{
    private final Map<SchemaTableName, List<Page>> pages = new HashMap<>();

    public synchronized void add(SchemaTableName schemaTableName, Page page)
    {
        if (!pages.containsKey(schemaTableName)) {
            pages.put(schemaTableName, new ArrayList<>());
        }

        List<Page> tablePages = pages.get(schemaTableName);
        tablePages.add(page);
    }

    public synchronized List<Page> getPages(SchemaTableName schemaTableName, int partNumber, int totalParts, List<Integer> columnIndexes)
    {
        checkState(pages.containsKey(schemaTableName));

        List<Page> tablePages = pages.get(schemaTableName);
        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();

        for (int i = partNumber; i < tablePages.size(); i += totalParts) {
            partitionedPages.add(reorderPage(tablePages.get(i), columnIndexes));
        }

        return partitionedPages.build();
    }

    public synchronized void remove(SchemaTableName schemaTableName)
    {
        checkState(pages.containsKey(schemaTableName));
        pages.remove(schemaTableName);
    }

    public synchronized void rename(SchemaTableName oldSchemaTableName, SchemaTableName newSchemaTableName)
    {
        checkState(pages.containsKey(oldSchemaTableName));
        pages.put(newSchemaTableName, pages.remove(oldSchemaTableName));
    }

    public synchronized boolean contains(SchemaTableName schemaTableName)
    {
        return pages.containsKey(schemaTableName);
    }

    private static Page reorderPage(Page page, List<Integer> columnIndexes)
    {
        Block[] blocks = page.getBlocks();
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = blocks[columnIndexes.get(i)];
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }
}
