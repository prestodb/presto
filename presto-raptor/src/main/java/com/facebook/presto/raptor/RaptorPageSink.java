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

import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageOutputHandle;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private final String nodeId;
    private final StorageManager storageManager;
    private final StorageOutputHandle storageOutputHandle;
    private final int sampleWeightField;
    private final long maxRowCount;

    private StoragePageSink storagePageSink;
    private int rowCount;

    public RaptorPageSink(
            String nodeId,
            StorageManager storageManager,
            List<Long> columnIds,
            List<Type> columnTypes,
            Optional<Long> sampleWeightColumnId)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.storageOutputHandle = storageManager.createStorageOutputHandle(columnIds, columnTypes);
        this.maxRowCount = storageManager.getMaxRowCount();
        checkNotNull(sampleWeightColumnId, "sampleWeightColumnId is null");
        this.sampleWeightField = columnIds.indexOf(sampleWeightColumnId.orElse(-1L));

        this.storagePageSink = createPageSink(storageManager, storageOutputHandle);
        this.rowCount = 0;
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        if (rowCount >= maxRowCount) {
            storagePageSink.close();
            storagePageSink = createPageSink(storageManager, storageOutputHandle);
            rowCount = 0;
        }

        if (sampleWeightField < 0) {
            appendPage(page);
            return;
        }

        // Create a page with the sampleWeightBlock at the sampleWeightField index
        checkArgument(page.getPositionCount() == sampleWeightBlock.getPositionCount(), "position count of page and sampleWeightBlock must match");
        int outputChannelCount = page.getChannelCount() + 1;
        Block[] blocks = new Block[outputChannelCount];
        blocks[sampleWeightField] = sampleWeightBlock;

        int pageChannel = 0;
        for (int channel = 0; channel < outputChannelCount; channel++) {
            if (channel == sampleWeightField) {
                continue;
            }
            blocks[channel] = page.getBlock(pageChannel);
            pageChannel++;
        }
        appendPage(new Page(blocks));
    }

    private void appendPage(Page page)
    {
        storagePageSink.appendPage(page);
        rowCount += page.getPositionCount();
    }

    @Override
    public String commit()
    {
        storagePageSink.close();
        List<UUID> shardUuids = storageManager.commit(storageOutputHandle);
        // Format of each fragment: nodeId:shardUuid1,shardUuid2,shardUuid3
        return Joiner.on(':').join(nodeId, Joiner.on(",").join(shardUuids));

    }

    private static StoragePageSink createPageSink(StorageManager storageManager, StorageOutputHandle storageOutputHandle)
    {
        return storageManager.createStoragePageSink(storageOutputHandle);
    }
}
