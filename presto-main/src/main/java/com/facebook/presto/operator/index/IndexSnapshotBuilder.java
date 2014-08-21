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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.UnloadedIndexKeyRecordSet.UnloadedIndexKeyRecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class IndexSnapshotBuilder
{
    private final List<Type> types;
    private final List<Type> missingKeysTypes;
    private final int expectedPositions;
    private final List<Integer> indexChannels;
    private final long maxMemoryInBytes;
    private final List<Integer> missingKeysChannels;

    private final OperatorContext bogusOperatorContext;
    private PagesIndex currentPagesIndex;

    private PagesIndex missingKeysIndex;
    private LookupSource missingKeys;

    private final List<Page> pages = new ArrayList<>();
    private long memoryInBytes;

    public IndexSnapshotBuilder(List<Type> types,
            List<Integer> indexChannels,
            DriverContext driverContext,
            DataSize maxMemoryInBytes,
            int expectedPositions)
    {
        this.types = ImmutableList.copyOf(types);
        this.expectedPositions = expectedPositions;
        this.indexChannels = indexChannels;
        this.maxMemoryInBytes = maxMemoryInBytes.toBytes();

        ImmutableList.Builder<Type> missingKeysTypes = ImmutableList.builder();
        ImmutableList.Builder<Integer> missingKeysChannels = ImmutableList.builder();
        for (int i = 0; i < indexChannels.size(); i++) {
            Integer outputIndexChannel = indexChannels.get(i);
            missingKeysTypes.add(types.get(outputIndexChannel));
            missingKeysChannels.add(i);
        }
        this.missingKeysTypes = missingKeysTypes.build();
        this.missingKeysChannels = missingKeysChannels.build();

        // create a bogus operator context with unlimited memory for the pages index
        bogusOperatorContext = new TaskContext(driverContext.getTaskId(), driverContext.getExecutor(), driverContext.getSession(), new DataSize(Long.MAX_VALUE, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext()
                .addOperatorContext(0, "operator");

        this.currentPagesIndex = new PagesIndex(types, expectedPositions, bogusOperatorContext);
        this.missingKeysIndex = new PagesIndex(missingKeysTypes.build(), expectedPositions, bogusOperatorContext);
        this.missingKeys = missingKeysIndex.createLookupSource(this.missingKeysChannels);
    }

    public boolean isEmpty()
    {
        return currentPagesIndex.getPositionCount() == 0 && pages.isEmpty();
    }

    public boolean isMemoryExceeded()
    {
        return memoryInBytes > maxMemoryInBytes;
    }

    public boolean tryAddPage(Page page)
    {
        memoryInBytes += page.getDataSize().toBytes();
        if (isMemoryExceeded()) {
            return false;
        }
        pages.add(page);
        return true;
    }

    public IndexSnapshot createIndexSnapshot(UnloadedIndexKeyRecordSet unloadedKeysRecordSet)
    {
        checkState(!isMemoryExceeded(), "Max memory exceeded");
        for (Page page : pages) {
            currentPagesIndex.addPage(page);
        }
        pages.clear();

        LookupSource lookupSource = currentPagesIndex.createLookupSource(indexChannels);

        // Build a page containing the keys that produced no output rows, so in future requests can skip these keys
        PageBuilder missingKeysPageBuilder = new PageBuilder(missingKeysIndex.getTypes());
        UnloadedIndexKeyRecordCursor unloadedKeyRecordCursor = unloadedKeysRecordSet.cursor();
        while (unloadedKeyRecordCursor.advanceNextPosition()) {
            Block[] blocks = unloadedKeyRecordCursor.getBlocks();
            int position = unloadedKeyRecordCursor.getPosition();
            if (lookupSource.getJoinPosition(position, blocks) < 0) {
                for (int i = 0; i < blocks.length; i++) {
                    Block block = blocks[i];
                    Type type = unloadedKeyRecordCursor.getType(i);
                    type.appendTo(block, position, missingKeysPageBuilder.getBlockBuilder(i));
                }
            }
        }

        // only update missing keys if we have new missing keys
        if (!missingKeysPageBuilder.isEmpty()) {
            missingKeysIndex.addPage(missingKeysPageBuilder.build());
            missingKeys = missingKeysIndex.createLookupSource(missingKeysChannels);
        }

        return new IndexSnapshot(lookupSource, missingKeys);
    }

    public void reset()
    {
        memoryInBytes = 0;
        pages.clear();
        currentPagesIndex = new PagesIndex(types, expectedPositions, bogusOperatorContext);
        missingKeysIndex = new PagesIndex(missingKeysTypes, expectedPositions, bogusOperatorContext);
    }
}
