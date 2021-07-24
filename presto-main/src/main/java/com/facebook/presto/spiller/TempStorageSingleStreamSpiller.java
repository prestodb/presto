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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PageDataOutput;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.spiller.SpillCipher;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.InputStreamSliceInput;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getTempStorageSpillerBufferSize;
import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class TempStorageSingleStreamSpiller
        implements SingleStreamSpiller
{
    private final TempStorage tempStorage;
    private final PagesSerde serde;
    private final ListeningExecutorService executor;
    private final SpillerStats spillerStats;
    private final SpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;
    private final Optional<SpillCipher> spillCipher;
    private final int maxBufferSizeInBytes;
    private final TempDataOperationContext tempDataOperationContext;

    private final Closer closer = Closer.create();

    private boolean writable = true;
    private boolean committed;
    private final TempDataSink dataSink;
    private TempStorageHandle tempStorageHandle;
    private int bufferedBytes;
    private List<DataOutput> bufferedPages = new ArrayList<>();
    private long spilledPagesInMemorySize;
    private ListenableFuture<?> spillInProgress = Futures.immediateFuture(null);

    public TempStorageSingleStreamSpiller(
            TempStorage tempStorage,
            PagesSerde serde,
            ListeningExecutorService executor,
            SpillerStats spillerStats,
            SpillContext spillContext,
            LocalMemoryContext memoryContext,
            Optional<SpillCipher> spillCipher)
    {
        this.tempStorage = requireNonNull(tempStorage, "tempStorage is null");
        this.serde = requireNonNull(serde, "serde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.localSpillContext = spillContext.newLocalSpillContext();
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.spillCipher = requireNonNull(spillCipher, "spillCipher is null");
        checkState(!spillCipher.isPresent() || !spillCipher.get().isDestroyed(), "spillCipher is already destroyed");
        this.spillCipher.ifPresent(cipher -> closer.register(cipher::destroy));

        Session session = spillContext.getSession();
        this.tempDataOperationContext = new TempDataOperationContext(
                session.getSource(),
                session.getQueryId().getId(),
                session.getClientInfo(),
                Optional.of(session.getClientTags()),
                session.getIdentity());

        this.maxBufferSizeInBytes = toIntExact(getTempStorageSpillerBufferSize(session).toBytes());

        try {
            dataSink = tempStorage.create(tempDataOperationContext);
            memoryContext.setBytes(dataSink.getRetainedSizeInBytes());
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill sink", e);
        }
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Page> pageIterator)
    {
        requireNonNull(pageIterator, "pageIterator is null");
        checkNoSpillInProgress();
        spillInProgress = executor.submit(() -> writePages(pageIterator));
        return spillInProgress;
    }

    @Override
    public long getSpilledPagesInMemorySize()
    {
        return spilledPagesInMemorySize;
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        checkNoSpillInProgress();
        return readPages();
    }

    @Override
    public ListenableFuture<List<Page>> getAllSpilledPages()
    {
        return executor.submit(() -> ImmutableList.copyOf(getSpilledPages()));
    }

    @Override
    public void commit()
    {
        if (committed) {
            return;
        }

        try {
            if (!bufferedPages.isEmpty()) {
                flushBufferedPages();
            }
            tempStorageHandle = dataSink.commit();
            committed = true;
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to commit spill file", e);
        }
    }

    private void writePages(Iterator<Page> pageIterator)
    {
        checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
        checkState(!committed, "Spilling no longer allowed. Spill file is already committed");
        while (pageIterator.hasNext()) {
            Page page = pageIterator.next();
            spilledPagesInMemorySize += page.getSizeInBytes();
            // page serialization requires  page.getSizeInBytes() + Integer.BYTES to fit in an integer
            splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                    .map(serde::serialize)
                    .forEach(serializedPage -> {
                        long pageSize = serializedPage.getSizeInBytes();
                        localSpillContext.updateBytes(pageSize);
                        spillerStats.addToTotalSpilledBytes(pageSize);
                        PageDataOutput pageDataOutput = new PageDataOutput(serializedPage);
                        bufferedBytes += toIntExact(pageDataOutput.size());
                        bufferedPages.add(pageDataOutput);
                        if (bufferedBytes > maxBufferSizeInBytes) {
                            flushBufferedPages();
                        }
                    });
        }
        memoryContext.setBytes(bufferedBytes + dataSink.getRetainedSizeInBytes());
    }

    private Iterator<Page> readPages()
    {
        checkState(writable, "Repeated reads are disallowed to prevent potential resource leaks");
        writable = false;
        try {
            if (!committed) {
                commit();
            }

            checkState(committed, "Cannot read pages since spill file is not committed");
            InputStream input = closer.register(tempStorage.open(tempDataOperationContext, tempStorageHandle));
            Iterator<Page> deserializedPages = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input));
            Iterator<Page> compactPages = transform(deserializedPages, Page::compact);
            return closeWhenExhausted(compactPages, input);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public void close()
    {
        if (writable) {
            closer.register(() -> dataSink.rollback());
        }
        if (tempStorageHandle != null) {
            closer.register(() -> tempStorage.remove(tempDataOperationContext, tempStorageHandle));
        }

        closer.register(localSpillContext);
        closer.register(() -> memoryContext.setBytes(0));
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to close spiller", e);
        }
    }

    private void flushBufferedPages()
    {
        try {
            dataSink.write(bufferedPages);
        }
        catch (UncheckedIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }

        bufferedPages.clear();
        bufferedBytes = 0;

        memoryContext.setBytes(dataSink.getRetainedSizeInBytes());
    }

    private void checkNoSpillInProgress()
    {
        checkState(spillInProgress.isDone(), "spill in progress");
    }

    private static <T> Iterator<T> closeWhenExhausted(Iterator<T> iterator, Closeable resource)
    {
        requireNonNull(iterator, "iterator is null");
        requireNonNull(resource, "resource is null");

        return new AbstractIterator<T>()
        {
            @Override
            protected T computeNext()
            {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                try {
                    resource.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return endOfData();
            }
        };
    }
}
