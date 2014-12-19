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

import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.raptor.storage.StorageOutputHandle;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private final String nodeId;
    private final StorageManager storageManager;
    private final StoragePageSink storagePageSink;
    private final StorageOutputHandle storageOutputHandle;
    private final int sampleWeightField;

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
        this.storagePageSink = storageManager.getStoragePageSink(storageOutputHandle);

        checkNotNull(sampleWeightColumnId, "sampleWeightColumnId is null");
        this.sampleWeightField = columnIds.indexOf(sampleWeightColumnId.or(-1L));
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        if (sampleWeightField < 0) {
            storagePageSink.appendPage(page);
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
        storagePageSink.appendPage(new Page(blocks));
    }

    @Override
    public String commit()
    {
        UUID shardUuid = storageManager.commit(storageOutputHandle);
        return Joiner.on(':').join(nodeId, shardUuid);
    }
}
