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

import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.raptorx.storage.organization.PageIndexInfo;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_STORAGE_ERROR;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcFileWriter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class OrcFileWriter
        implements Closeable
{
    private final PageBuilder pageBuilder;
    private final OrcWriter orcWriter;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;

    public OrcFileWriter(
            long chunkId,
            List<Long> columnIds,
            List<Type> columnTypes,
            CompressionType compressionType,
            File target,
            TypeManager typeManager,
            OrcWriterStats stats)
    {
        StorageTypeConverter converter = new StorageTypeConverter(typeManager);
        List<Type> storageTypes = columnTypes.stream()
                .map(converter::toStorageType)
                .collect(toImmutableList());

        this.pageBuilder = new PageBuilder(storageTypes);

        Map<String, String> metadata = OrcFileMetadata.from(chunkId, columnIds, columnTypes).toMap();
        this.orcWriter = createOrcFileWriter(target, columnIds, storageTypes, compressionType, metadata, stats);
    }

    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            appendPage(page);
        }
    }

    public void appendPages(List<Page> pages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");

        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = pages.get(pageIndexes[i]);
            int position = positionIndexes[i];
            appendPositionTo(page, position, pageBuilder);

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    public void appendPageIndexInfos(List<PageIndexInfo> pageIndexInfo)
    {
        for (int i = 0; i < pageIndexInfo.size(); i++) {
            Page page = pageIndexInfo.get(i).getCurrentPage();
            int position = pageIndexInfo.get(i).getCurrentPosition();
            appendPositionTo(page, position, pageBuilder);

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            orcWriter.close();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to close writer", e);
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private void appendPage(Page page)
    {
        rowCount += page.getPositionCount();
        uncompressedSize += page.getLogicalSizeInBytes();

        try {
            orcWriter.write(page);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to write data", e);
        }
    }

    private static void appendPositionTo(Page page, int position, PageBuilder pageBuilder)
    {
        pageBuilder.declarePosition();
        for (int i = 0; i < page.getChannelCount(); i++) {
            Type type = pageBuilder.getType(i);
            Block block = page.getBlock(i);
            BlockBuilder output = pageBuilder.getBlockBuilder(i);
            type.appendTo(block, position, output);
        }
    }
}
