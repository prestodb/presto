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
package com.facebook.presto.spiller;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PageDataOutput;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.storage.SerializedStorageHandle;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import io.airlift.slice.InputStreamSliceInput;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_SPILL_FAILURE;
import static com.google.common.collect.Iterators.transform;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * TempStorageStandaloneSpiller is a stateless spiller that provides basic spill
 * capabilities like spill, read, and remove. It operates over a storage handle
 * which can be serialized and passed around. Since the spiller does not maintain
 * any state internally, same instance can be used to operate on multiple spill files
 */
public class TempStorageStandaloneSpiller
        implements StandaloneSpiller
{
    private final TempDataOperationContext tempDataOperationContext;
    private final TempStorage tempStorage;
    private final PagesSerde serde;
    private final SpillerStats spillerStats;
    private final int maxBufferSizeInBytes;

    public TempStorageStandaloneSpiller(
            TempDataOperationContext tempDataOperationContext,
            TempStorage tempStorage,
            PagesSerde serde,
            SpillerStats spillerStats,
            int maxBufferSizeInBytes)
    {
        this.tempDataOperationContext = requireNonNull(tempDataOperationContext, "tempDataOperationContext is null");
        this.tempStorage = requireNonNull(tempStorage, "tempStorage is null");
        this.serde = requireNonNull(serde, "serde is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.maxBufferSizeInBytes = maxBufferSizeInBytes;
    }

    public SerializedStorageHandle spill(Iterator<Page> pageIterator)
    {
        List<DataOutput> bufferedPages = new ArrayList<>();
        int bufferedBytes = 0;
        IOException ioException = null;
        TempDataSink tempDataSink = null;
        try {
            tempDataSink = tempStorage.create(tempDataOperationContext);
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                List<Page> splitPages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
                for (Page splitPage : splitPages) {
                    SerializedPage serializedPage = serde.serialize(splitPage);
                    spillerStats.addToTotalSpilledBytes(serializedPage.getSizeInBytes());
                    PageDataOutput pageDataOutput = new PageDataOutput(serializedPage);
                    bufferedBytes += toIntExact(pageDataOutput.size());
                    bufferedPages.add(pageDataOutput);
                    if (bufferedBytes > maxBufferSizeInBytes) {
                        flushBufferedPages(tempDataSink, bufferedPages);
                        bufferedBytes = 0;
                    }
                }
            }

            // Flush remaining buffered pages
            if (!bufferedPages.isEmpty()) {
                flushBufferedPages(tempDataSink, bufferedPages);
            }
            TempStorageHandle tempStorageHandle = tempDataSink.commit();
            return new SerializedStorageHandle(tempStorage.serializeHandle(tempStorageHandle));
        }
        catch (IOException e) {
            ioException = e;
            try {
                if (tempDataSink != null) {
                    tempDataSink.rollback();
                }
            }
            catch (IOException exception) {
                if (ioException != exception) {
                    ioException.addSuppressed(exception);
                }
            }
        }
        finally {
            try {
                if (tempDataSink != null) {
                    tempDataSink.close();
                }
            }
            catch (IOException e) {
                if (ioException == null) {
                    ioException = e;
                }
                else if (ioException != e) {
                    ioException.addSuppressed(e);
                }
                throw new PrestoException(GENERIC_SPILL_FAILURE, "Failed to spill pages", ioException);
            }
        }

        throw new PrestoException(GENERIC_SPILL_FAILURE, "Failed to spill pages", ioException);
    }

    private void flushBufferedPages(TempDataSink tempDataSink, List<DataOutput> bufferedPages)
    {
        try {
            tempDataSink.write(bufferedPages);
        }
        catch (UncheckedIOException | IOException e) {
            throw new PrestoException(GENERIC_SPILL_FAILURE, "Failed to spill pages", e);
        }

        bufferedPages.clear();
    }

    public Iterator<Page> getSpilledPages(SerializedStorageHandle storageHandle)
    {
        try {
            TempStorageHandle tempStorageHandle = tempStorage.deserialize(storageHandle.getSerializedStorageHandle());
            InputStream inputStream = tempStorage.open(tempDataOperationContext, tempStorageHandle);
            Iterator<Page> deserializedPages = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(inputStream));
            return transform(deserializedPages, Page::compact);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_SPILL_FAILURE, "Failed to read spilled pages", e);
        }
    }

    public void remove(SerializedStorageHandle storageHandle)
    {
        try {
            tempStorage.remove(tempDataOperationContext, tempStorage.deserialize(storageHandle.getSerializedStorageHandle()));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_SPILL_FAILURE, "Failed to delete spill file", e);
        }
    }
}
