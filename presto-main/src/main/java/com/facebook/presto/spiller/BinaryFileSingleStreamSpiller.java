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

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeUtil;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class BinaryFileSingleStreamSpiller
        implements SingleStreamSpiller
{
    @GuardedBy("this")
    private final Path targetFile;

    @GuardedBy("this")
    private final PagesSerde serde;

    @GuardedBy("this")
    private final ListeningExecutorService executor;

    @GuardedBy("this")
    private final Queue<CompletableFuture<?>> scheduledSpills = new ArrayDeque<>();

    @GuardedBy("output") // this variable is used in executor's threads as opposed to other variable
    private final SliceOutput output;

    private final AtomicLong spilledBytes;

    @GuardedBy("this")
    private boolean outputClosed;

    @GuardedBy("this")
    private boolean closed;

    public BinaryFileSingleStreamSpiller(
            PagesSerde serde,
            ListeningExecutorService executor,
            Path spillPath,
            AtomicLong spilledBytes)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spilledBytes = requireNonNull(spilledBytes, "spilledBytes is null");
        try {
            targetFile = Files.createTempFile(spillPath, "presto-spill", ".bin");
            output = new OutputStreamSliceOutput(new BufferedOutputStream(new FileOutputStream(targetFile.toFile())));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill directory", e);
        }
    }

    @Override
    public synchronized CompletableFuture<?> spill(Iterator<Page> pageIterator)
    {
        CompletableFuture<?> future = MoreFutures.toCompletableFuture(executor.submit(
            () -> writePages(pageIterator)
        ));
        scheduledSpills.add(future);
        removeFinishedFutures();
        return future;
    }

    @Override
    public synchronized CompletableFuture<?> spill(Page page)
    {
        CompletableFuture<?> future = MoreFutures.toCompletableFuture(executor.submit(
                () -> writePage(page)
        ));
        scheduledSpills.add(future);
        removeFinishedFutures();
        return future;
    }

    private void writePage(Page page)
    {
        // don't synchronize on this, to prevent blocking whole class on the duration of spill
        synchronized (output) {
            spilledBytes.addAndGet(PagesSerdeUtil.writePages(serde, output, page));
        }
    }

    private void writePages(Iterator<Page> pageIterator)
    {
        // don't synchronize on this, to prevent blocking whole class on the duration of spill
        synchronized (output) {
            spilledBytes.addAndGet(PagesSerdeUtil.writePages(serde, output, pageIterator));
        }
    }

    @Override
    public synchronized Iterator<Page> getSpilledPages()
    {
        waitForSpillsToFinish();
        checkState(scheduledSpills.isEmpty(), "Can not read if writes have not finished");
        return readPages();
    }

    private synchronized Iterator<Page> readPages()
    {
        try {
            output.flush();
            output.close();
            outputClosed = true;
            InputStream input = new BufferedInputStream(new FileInputStream(targetFile.toFile()));
            return PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public synchronized void close()
    {
        try {
            if (!closed) {
                if (!outputClosed) {
                    output.close();
                }
                Files.delete(targetFile);
                closed = true;
            }
        }
        catch (IOException e) {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    String.format("Failed to delete directory [%s]", targetFile),
                    e);
        }
    }

    private synchronized void waitForSpillsToFinish()
    {
        while (scheduledSpills.size() > 0) {
            getFutureValue(scheduledSpills.remove());
        }
    }

    private synchronized void removeFinishedFutures()
    {
        while (scheduledSpills.size() > 0 && scheduledSpills.peek().isDone()) {
            scheduledSpills.remove();
        }
    }
}
