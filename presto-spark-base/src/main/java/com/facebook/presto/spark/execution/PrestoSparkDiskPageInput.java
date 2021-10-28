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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkStorageHandle;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.InputStreamSliceInput;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

import static com.facebook.presto.spark.SparkErrorCode.STORAGE_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPages;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;

public class PrestoSparkDiskPageInput
        implements PrestoSparkPageInput
{
    private static final Logger log = Logger.get(PrestoSparkDiskPageInput.class);

    private final PagesSerde pagesSerde;
    private final TempStorage tempStorage;
    private final TempDataOperationContext tempDataOperationContext;
    private final PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager;
    private final StageId stageId;
    private final PlanNodeId planNodeId;
    private final List<List<PrestoSparkStorageHandle>> broadcastTableFilesInfo;

    @GuardedBy("this")
    private List<Iterator<Page>> pageIterators;
    @GuardedBy("this")
    private int currentIteratorIndex;

    public PrestoSparkDiskPageInput(
            PagesSerde pagesSerde,
            TempStorage tempStorage,
            TempDataOperationContext tempDataOperationContext,
            PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager,
            StageId stageId,
            PlanNodeId planNodeId,
            List<List<PrestoSparkStorageHandle>> broadcastTableFilesInfo)
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.tempStorage = requireNonNull(tempStorage, "tempStorage is null");
        this.tempDataOperationContext = requireNonNull(tempDataOperationContext, "tempDataOperationContext is null");
        this.prestoSparkBroadcastTableCacheManager = requireNonNull(prestoSparkBroadcastTableCacheManager, "prestoSparkBroadcastTableCacheManager is null");
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.broadcastTableFilesInfo = requireNonNull(broadcastTableFilesInfo, "broadcastTableFilesInfo is null");
    }

    @Override
    public Page getNextPage()
    {
        Page page = null;
        synchronized (this) {
            while (page == null) {
                if (currentIteratorIndex >= getPageIterators().size()) {
                    return null;
                }
                Iterator<Page> currentIterator = getPageIterators().get(currentIteratorIndex);
                if (currentIterator.hasNext()) {
                    page = currentIterator.next();
                }
                else {
                    currentIteratorIndex++;
                }
            }
        }
        return page;
    }

    private List<Iterator<Page>> getPageIterators()
    {
        if (pageIterators == null) {
            pageIterators = getPages(broadcastTableFilesInfo, tempStorage, tempDataOperationContext, prestoSparkBroadcastTableCacheManager, stageId, planNodeId);
        }
        return pageIterators;
    }

    private List<Iterator<Page>> getPages(
            List<List<PrestoSparkStorageHandle>> broadcastTableFilesInfo,
            TempStorage tempStorage,
            TempDataOperationContext tempDataOperationContext,
            PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager,
            StageId stageId,
            PlanNodeId planNodeId)
    {
        // Try to get table from cache
        List<List<Page>> pages = prestoSparkBroadcastTableCacheManager.getCachedBroadcastTable(stageId, planNodeId);
        if (pages == null) {
            pages = broadcastTableFilesInfo.stream()
                    .map(tableFiles -> {
                        List<SerializedPage> serializedPages = loadBroadcastTable(tableFiles, tempStorage, tempDataOperationContext);
                        return serializedPages.stream().map(serializedPage -> pagesSerde.deserialize(serializedPage)).collect(toImmutableList());
                    }).collect(toImmutableList());

            // Cache deserialized pages
            prestoSparkBroadcastTableCacheManager.cache(stageId, planNodeId, pages);
        }

        return pages.stream().map(List::iterator).collect(toImmutableList());
    }

    private List<SerializedPage> loadBroadcastTable(
            List<PrestoSparkStorageHandle> broadcastTaskFilesInfo,
            TempStorage tempStorage,
            TempDataOperationContext tempDataOperationContext)
    {
        try {
            CRC32 checksum = new CRC32();
            ImmutableList.Builder<SerializedPage> pages = ImmutableList.builder();
            List<PrestoSparkStorageHandle> broadcastTaskFilesInfoCopy = new ArrayList<>(broadcastTaskFilesInfo);
            shuffle(broadcastTaskFilesInfoCopy);
            for (PrestoSparkTaskOutput taskFileInfo : broadcastTaskFilesInfoCopy) {
                checksum.reset();
                PrestoSparkStorageHandle prestoSparkStorageHandle = (PrestoSparkStorageHandle) taskFileInfo;
                TempStorageHandle tempStorageHandle = tempStorage.deserialize(prestoSparkStorageHandle.getSerializedStorageHandle());
                log.info("Reading path: " + tempStorageHandle.toString());
                try (InputStream inputStream = tempStorage.open(tempDataOperationContext, tempStorageHandle);
                        InputStreamSliceInput inputStreamSliceInput = new InputStreamSliceInput(inputStream)) {
                    Iterator<SerializedPage> pagesIterator = readSerializedPages(inputStreamSliceInput);
                    while (pagesIterator.hasNext()) {
                        SerializedPage page = pagesIterator.next();
                        checksum.update(page.getSlice().byteArray(), page.getSlice().byteArrayOffset(), page.getSlice().length());
                        pages.add(page);
                    }
                }

                if (checksum.getValue() != prestoSparkStorageHandle.getChecksum()) {
                    throw new PrestoException(STORAGE_ERROR, "Disk page checksum does not match. " +
                            "Data seems to be corrupted on disk for file " + tempStorageHandle.toString());
                }
            }
            return pages.build();
        }
        catch (UncheckedIOException | IOException e) {
            throw new PrestoException(STORAGE_ERROR, "Unable to read data from disk: ", e);
        }
    }

    public long getRetainedSizeInBytes()
    {
        return prestoSparkBroadcastTableCacheManager.getBroadcastTableSizeInBytes(stageId, planNodeId);
    }
}
