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
package com.facebook.presto.server.protocol;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.SpoolingOutputBuffer;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.presto.common.RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_BYTES;
import static com.facebook.presto.common.RuntimeMetricName.COORDINATOR_BUFFER_DRAINED_PAGES;
import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.util.Objects.requireNonNull;

/**
 * A coordinator-side gate between the {@link ExchangeClient} and the client result path
 * in {@link Query}. Internally uses a {@link SpoolingOutputBuffer} for memory-to-SSD
 * tiered storage.
 *
 * <p>Pages from the ExchangeClient are drained into the SpoolingOutputBuffer as they
 * arrive. While the gate is closed (before {@link #release()}), {@link #pollPage()}
 * returns null so the client sees no results and {@code hasProducedResult} stays false.
 * When the query finishes successfully, {@link #release()} opens the gate and pages
 * flow from the SpoolingOutputBuffer through a read-ahead buffer to the client.</p>
 */
@ThreadSafe
class CoordinatorResultBuffer
{
    private static final long READ_BATCH_SIZE = 8 * 1024 * 1024; // 8 MB

    private final ExchangeClient exchangeClient;
    private final SpoolingOutputBuffer storageBuffer;
    private final RuntimeStats runtimeStats;
    private final OutputBufferId outputBufferId = new OutputBufferId(0);

    @GuardedBy("this")
    private final Deque<SerializedPage> readAheadBuffer = new ArrayDeque<>();

    @GuardedBy("this")
    private long readSequenceId;

    @GuardedBy("this")
    private boolean released;

    @GuardedBy("this")
    private boolean discarded;

    @GuardedBy("this")
    private boolean exchangeClientDrained;

    @GuardedBy("this")
    private boolean storageBufferComplete;

    @GuardedBy("this")
    private ListenableFuture<BufferResult> pendingStorageRead;

    CoordinatorResultBuffer(ExchangeClient exchangeClient, SpoolingOutputBuffer storageBuffer, RuntimeStats runtimeStats)
    {
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        this.storageBuffer = requireNonNull(storageBuffer, "storageBuffer is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    /**
     * Drains all available pages from the ExchangeClient into the SpoolingOutputBuffer.
     * When the ExchangeClient is closed (no more data), signals no-more-pages to the
     * storage buffer.
     */
    public synchronized void drainExchangeClient()
    {
        if (exchangeClientDrained || discarded) {
            return;
        }

        ImmutableList.Builder<SerializedPage> pages = ImmutableList.builder();
        SerializedPage page;
        while ((page = exchangeClient.pollPage()) != null) {
            pages.add(page);
        }

        List<SerializedPage> pageList = pages.build();
        if (!pageList.isEmpty()) {
            storageBuffer.enqueue(Lifespan.taskWide(), pageList);
            long totalBytes = 0;
            for (SerializedPage serializedPage : pageList) {
                totalBytes += serializedPage.getSizeInBytes();
            }
            runtimeStats.addMetricValue(COORDINATOR_BUFFER_DRAINED_PAGES, NONE, pageList.size());
            runtimeStats.addMetricValue(COORDINATOR_BUFFER_DRAINED_BYTES, BYTE, totalBytes);
        }

        if (exchangeClient.isClosed()) {
            storageBuffer.setNoMorePages();
            exchangeClientDrained = true;
        }
    }

    /**
     * Polls for the next page of results.
     *
     * <p>If discarded: returns null.
     * If not released: drains the exchange client (to relieve backpressure) and returns null.
     * If released: drains any newly arrived pages, returns from the read-ahead buffer,
     * or refills from the SpoolingOutputBuffer once the exchange client is fully drained.</p>
     *
     * <p>This method is non-blocking: reads from the storage buffer are initiated
     * asynchronously and checked on subsequent calls, so the monitor is never held
     * while waiting for I/O.</p>
     */
    public synchronized SerializedPage pollPage()
    {
        if (discarded) {
            return null;
        }

        if (!released) {
            drainExchangeClient();
            return null;
        }

        // Pages may still be in transit even after the query transitions to FINISHED
        drainExchangeClient();

        if (!readAheadBuffer.isEmpty()) {
            return readAheadBuffer.poll();
        }

        // Can't read from storage buffer until setNoMorePages() has been called
        if (!exchangeClientDrained) {
            return null;
        }

        if (pendingStorageRead != null) {
            Optional<BufferResult> optionalResult = tryGetFutureValue(pendingStorageRead);
            if (!optionalResult.isPresent()) {
                return null;
            }
            pendingStorageRead = null;
            processStorageReadResult(optionalResult.get());
            if (!readAheadBuffer.isEmpty()) {
                return readAheadBuffer.poll();
            }
            return null;
        }

        pendingStorageRead = storageBuffer.get(outputBufferId, readSequenceId, READ_BATCH_SIZE);

        // Check if completed immediately (common when data is in memory)
        Optional<BufferResult> optionalResult = tryGetFutureValue(pendingStorageRead);
        if (optionalResult.isPresent()) {
            pendingStorageRead = null;
            processStorageReadResult(optionalResult.get());
            if (!readAheadBuffer.isEmpty()) {
                return readAheadBuffer.poll();
            }
        }

        return null;
    }

    private void processStorageReadResult(BufferResult result)
    {
        List<SerializedPage> serializedPages = result.getSerializedPages();
        readSequenceId = result.getNextToken();
        if (!serializedPages.isEmpty()) {
            readAheadBuffer.addAll(serializedPages);
        }
        if (result.isBufferComplete()) {
            storageBufferComplete = true;
        }
    }

    /**
     * Opens the gate, allowing pages to flow to the client via {@link #pollPage()}.
     * Also eagerly drains any remaining pages from the ExchangeClient.
     */
    public synchronized void release()
    {
        released = true;
        drainExchangeClient();
    }

    /**
     * Discards all buffered data for a query retry. After this call, {@link #pollPage()}
     * returns null and the storage buffer is destroyed to free memory and storage files.
     */
    public synchronized void discardForRetry()
    {
        discarded = true;
        released = false;
        readAheadBuffer.clear();
        pendingStorageRead = null;
        storageBuffer.destroy();
    }

    /**
     * Returns true when the buffer has been released and all pages have been consumed.
     */
    public synchronized boolean isFinished()
    {
        return released && exchangeClientDrained && readAheadBuffer.isEmpty() && storageBufferComplete;
    }

    /**
     * Returns true if the buffer may still deliver pages to the client.
     * Returns false when the buffer has been discarded (nothing more to deliver)
     * or when all pages have been consumed normally.
     */
    public synchronized boolean hasRemainingData()
    {
        if (discarded) {
            return false;
        }
        return !isFinished();
    }
}
