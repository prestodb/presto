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
package io.prestosql.spiller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.prestosql.execution.buffer.PagesSerde;
import io.prestosql.execution.buffer.PagesSerdeUtil;
import io.prestosql.execution.buffer.SerializedPage;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.SpillContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.execution.buffer.PagesSerdeUtil.writeSerializedPage;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.prestosql.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class FileSingleStreamSpiller
        implements SingleStreamSpiller
{
    @VisibleForTesting
    static final int BUFFER_SIZE = 4 * 1024;

    private final FileHolder targetFile;
    private final Closer closer = Closer.create();
    private final PagesSerde serde;
    private final SpillerStats spillerStats;
    private final SpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;

    private final ListeningExecutorService executor;

    private boolean writable = true;
    private long spilledPagesInMemorySize;
    private ListenableFuture<?> spillInProgress = Futures.immediateFuture(null);

    public FileSingleStreamSpiller(
            PagesSerde serde,
            ListeningExecutorService executor,
            Path spillPath,
            SpillerStats spillerStats,
            SpillContext spillContext,
            LocalMemoryContext memoryContext)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.localSpillContext = spillContext.newLocalSpillContext();
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        // HACK!
        // The writePages() method is called in a separate thread pool and it's possible that
        // these spiller thread can run concurrently with the close() method.
        // Due to this race when the spiller thread is running, the driver thread:
        // 1. Can zero out the memory reservation even though the spiller thread physically holds onto that memory.
        // 2. Can close/delete the temp file(s) used for spilling, which doesn't have any visible side effects, but still not desirable.
        // To hack around the first issue we reserve the memory in the constructor and we release it in the close() method.
        // This means we start accounting for the memory before the spiller thread allocates it, and we release the memory reservation
        // before/after the spiller thread allocates that memory -- -- whether before or after depends on whether writePages() is in the
        // middle of execution when close() is called (note that this applies to both readPages() and writePages() methods).
        this.memoryContext.setBytes(BUFFER_SIZE);
        try {
            this.targetFile = closer.register(new FileHolder(Files.createTempFile(spillPath, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX)));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill file", e);
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

    private void writePages(Iterator<Page> pageIterator)
    {
        checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
        try (SliceOutput output = new OutputStreamSliceOutput(targetFile.newOutputStream(APPEND), BUFFER_SIZE)) {
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                spilledPagesInMemorySize += page.getSizeInBytes();
                SerializedPage serializedPage = serde.serialize(page);
                long pageSize = serializedPage.getSizeInBytes();
                localSpillContext.updateBytes(pageSize);
                spillerStats.addToTotalSpilledBytes(pageSize);
                writeSerializedPage(output, serializedPage);
            }
        }
        catch (UncheckedIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
    }

    private Iterator<Page> readPages()
    {
        checkState(writable, "Repeated reads are disallowed to prevent potential resource leaks");
        writable = false;

        try {
            InputStream input = closer.register(targetFile.newInputStream());
            Iterator<Page> pages = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input, BUFFER_SIZE));
            return closeWhenExhausted(pages, input);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public void close()
    {
        closer.register(localSpillContext);
        closer.register(() -> memoryContext.setBytes(0));
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to close spiller", e);
        }
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
