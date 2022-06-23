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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.presto.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class BufferTestUtils
{
    private BufferTestUtils() {}

    static final PagesSerde PAGES_SERDE = testingPagesSerde();
    static final Duration NO_WAIT = new Duration(0, MILLISECONDS);
    static final Duration MAX_WAIT = new Duration(1, SECONDS);
    private static final DataSize BUFFERED_PAGE_SIZE = new DataSize(PAGES_SERDE.serialize(createPage(42)).getRetainedSizeInBytes(), BYTE);

    static BufferResult getFuture(ListenableFuture<BufferResult> future, Duration maxWait)
    {
        Optional<BufferResult> bufferResult = tryGetFutureValue(future, (int) maxWait.toMillis(), MILLISECONDS);
        checkArgument(bufferResult.isPresent(), "bufferResult is empty");
        return bufferResult.get();
    }

    static void assertBufferResultEquals(List<? extends Type> types, BufferResult actual, BufferResult expected)
    {
        assertEquals(actual.getSerializedPages().size(), expected.getSerializedPages().size(), "page count");
        assertEquals(actual.getToken(), expected.getToken(), "token");
        for (int i = 0; i < actual.getSerializedPages().size(); i++) {
            Page actualPage = PAGES_SERDE.deserialize(actual.getSerializedPages().get(i));
            Page expectedPage = PAGES_SERDE.deserialize(expected.getSerializedPages().get(i));
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            PageAssertions.assertPageEquals(types, actualPage, expectedPage);
        }
        assertEquals(actual.isBufferComplete(), expected.isBufferComplete(), "buffer complete");
    }

    static BufferResult createBufferResult(String bufferId, long token, List<Page> pages)
    {
        checkArgument(!pages.isEmpty(), "pages is empty");
        return new BufferResult(
                bufferId,
                token,
                token + pages.size(),
                false,
                pages.stream()
                        .map(PAGES_SERDE::serialize)
                        .collect(Collectors.toList()));
    }

    public static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    static DataSize sizeOfPages(int count)
    {
        return new DataSize(BUFFERED_PAGE_SIZE.toBytes() * count, BYTE);
    }

    static BufferResult getBufferResult(OutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    // TODO: remove this after PR #7987 is landed
    static void acknowledgeBufferResult(OutputBuffer buffer, OutputBuffers.OutputBufferId bufferId, long sequenceId)
    {
        buffer.acknowledge(bufferId, sequenceId);
    }

    static ListenableFuture<?> enqueuePage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(Lifespan.taskWide(), ImmutableList.of(PAGES_SERDE.serialize(page)));
        ListenableFuture<?> future = buffer.isFull();
        assertFalse(future.isDone());
        return future;
    }

    static ListenableFuture<?> enqueuePage(OutputBuffer buffer, Page page, int partition)
    {
        buffer.enqueue(Lifespan.taskWide(), partition, ImmutableList.of(PAGES_SERDE.serialize(page)));
        ListenableFuture<?> future = buffer.isFull();
        assertFalse(future.isDone());
        return future;
    }

    public static void addPages(OutputBuffer buffer, List<Page> pages)
    {
        requireNonNull(pages, "pages is null");
        List<SerializedPage> serializedPages = pages.stream()
                .map(PAGES_SERDE::serialize)
                .collect(toImmutableList());
        buffer.enqueue(Lifespan.taskWide(), serializedPages);
    }

    public static void addPage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(Lifespan.taskWide(), ImmutableList.of(PAGES_SERDE.serialize(page)));
        assertTrue(buffer.isFull().isDone(), "Expected add page to not block");
    }

    public static void addPage(OutputBuffer buffer, Page page, int partition)
    {
        buffer.enqueue(Lifespan.taskWide(), partition, ImmutableList.of(PAGES_SERDE.serialize(page)));
        assertTrue(buffer.isFull().isDone(), "Expected add page to not block");
    }

    static void assertQueueState(
            OutputBuffer buffer,
            OutputBuffers.OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        assertEquals(
                getBufferInfo(buffer, bufferId),
                new BufferInfo(
                        bufferId,
                        false,
                        bufferedPages,
                        pagesSent,
                        new PageBufferInfo(
                                bufferId.getId(),
                                bufferedPages,
                                sizeOfPages(bufferedPages).toBytes(),
                                bufferedPages + pagesSent, // every page has one row
                                bufferedPages + pagesSent)));
    }

    static void assertQueueState(
            OutputBuffer buffer,
            int unassignedPages,
            OutputBuffers.OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getBuffers().stream().mapToInt(BufferInfo::getBufferedPages).sum();

        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        BufferInfo bufferInfo = outputBufferInfo.getBuffers().stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(
                bufferInfo,
                new BufferInfo(
                        bufferId,
                        false,
                        bufferedPages,
                        pagesSent,
                        new PageBufferInfo(
                                bufferId.getId(),
                                bufferedPages,
                                sizeOfPages(bufferedPages).toBytes(),
                                bufferedPages + pagesSent, // every page has one row
                                bufferedPages + pagesSent)));
    }

    @SuppressWarnings("ConstantConditions")
    static void assertQueueClosed(OutputBuffer buffer, OutputBuffers.OutputBufferId bufferId, int pagesSent)
    {
        BufferInfo bufferInfo = getBufferInfo(buffer, bufferId);
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    @SuppressWarnings("ConstantConditions")
    static void assertQueueClosed(OutputBuffer buffer, int unassignedPages, OutputBuffers.OutputBufferId bufferId, int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getBuffers().stream().mapToInt(BufferInfo::getBufferedPages).sum();
        assertEquals(
                outputBufferInfo.getTotalBufferedPages() - assignedPages,
                unassignedPages,
                "unassignedPages");

        BufferInfo bufferInfo = outputBufferInfo.getBuffers().stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
    }

    static void assertFinished(OutputBuffer buffer)
    {
        assertTrue(buffer.isFinished());
        for (BufferInfo bufferInfo : buffer.getInfo().getBuffers()) {
            assertTrue(bufferInfo.isFinished());
            assertEquals(bufferInfo.getBufferedPages(), 0);
        }
    }

    static void assertFutureIsDone(Future<?> future)
    {
        tryGetFutureValue(future, 5, SECONDS);
        assertTrue(future.isDone());
    }

    private static BufferInfo getBufferInfo(OutputBuffer buffer, OutputBuffers.OutputBufferId bufferId)
    {
        for (BufferInfo bufferInfo : buffer.getInfo().getBuffers()) {
            if (bufferInfo.getBufferId().equals(bufferId)) {
                return bufferInfo;
            }
        }
        return null;
    }
}
