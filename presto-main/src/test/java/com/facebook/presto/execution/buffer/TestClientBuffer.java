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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.ClientBuffer.PagesSupplier;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.SerializedPageReference.PagesReleasedListener;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferTestUtils.NO_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.PAGES_SERDE;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getFuture;
import static com.facebook.presto.execution.buffer.BufferTestUtils.sizeOfPages;
import static com.facebook.presto.execution.buffer.SerializedPageReference.dereferencePages;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestClientBuffer
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";
    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId BUFFER_ID = new OutputBufferId(33);
    private static final String INVALID_SEQUENCE_ID = "Invalid sequence id";
    private static final PagesReleasedListener NOOP_RELEASE_LISTENER = (lifespan, releasedPagesCount, releasedSizeInBytes) -> {};

    @Test
    public void testSimplePushBuffer()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);

        // add three pages to the buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        assertBufferInfo(buffer, 3, 0);

        // get the pages elements from the buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 3, 0);

        // acknowledge first three pages in the buffer
        buffer.getPages(3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertBufferInfo(buffer, 0, 3);

        // add 3 more pages
        for (int i = 3; i < 6; i++) {
            addPage(buffer, createPage(i));
        }
        assertBufferInfo(buffer, 3, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertBufferInfo(buffer, 3, 3);

        // set no more pages
        buffer.setNoMorePages();
        // state should not change
        assertBufferInfo(buffer, 3, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));
        assertBufferInfo(buffer, 2, 4);

        // remove last pages from, should not be finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 5, sizeOfPages(30), NO_WAIT), bufferResult(5, createPage(5)));
        assertBufferInfo(buffer, 1, 5);

        // acknowledge all pages from the buffer, should return a finished buffer result
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 6, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));
        assertBufferInfo(buffer, 0, 6);

        // buffer is not destroyed until explicitly destroyed
        buffer.destroy();
        assertBufferDestroyed(buffer, 6);
    }

    @Test
    public void testSimplePullBuffer()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);

        // create a page supplier with 3 initial pages
        TestingPagesSupplier supplier = new TestingPagesSupplier();
        for (int i = 0; i < 3; i++) {
            supplier.addPage(createPage(i));
        }
        assertEquals(supplier.getBufferedPages(), 3);

        // get the pages elements from the buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, supplier, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // 3 pages are moved to the client buffer, but not acknowledged yet
        assertEquals(supplier.getBufferedPages(), 0);
        assertBufferInfo(buffer, 3, 0);

        // acknowledge first three pages in the buffer
        ListenableFuture<BufferResult> pendingRead = buffer.getPages(3, sizeOfPages(1));
        // pages now acknowledged
        assertEquals(supplier.getBufferedPages(), 0);
        assertBufferInfo(buffer, 0, 3);
        assertFalse(pendingRead.isDone());

        // add 3 more pages
        for (int i = 3; i < 6; i++) {
            supplier.addPage(createPage(i));
        }
        assertEquals(supplier.getBufferedPages(), 3);

        // notify the buffer that there is more data, and verify previous read completed
        buffer.loadPagesIfNecessary(supplier);
        assertBufferResultEquals(TYPES, getFuture(pendingRead, NO_WAIT), bufferResult(3, createPage(3)));
        // 1 page wad moved to the client buffer, but not acknowledged yet
        assertEquals(supplier.getBufferedPages(), 2);
        assertBufferInfo(buffer, 1, 3);

        // set no more pages
        supplier.setNoMorePages();
        // state should not change
        assertEquals(supplier.getBufferedPages(), 2);
        assertBufferInfo(buffer, 1, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, supplier, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));
        assertBufferInfo(buffer, 1, 4);
        assertEquals(supplier.getBufferedPages(), 1);

        // remove last pages from, should not be finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, supplier, 5, sizeOfPages(30), NO_WAIT), bufferResult(5, createPage(5)));
        assertBufferInfo(buffer, 1, 5);
        assertEquals(supplier.getBufferedPages(), 0);

        // acknowledge all pages from the buffer, should return a finished buffer result
        assertBufferResultEquals(TYPES, getBufferResult(buffer, supplier, 6, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));
        assertBufferInfo(buffer, 0, 6);
        assertEquals(supplier.getBufferedPages(), 0);

        // buffer is not destroyed until explicitly destroyed
        buffer.destroy();
        assertBufferDestroyed(buffer, 6);
    }

    @Test
    public void testDuplicateRequests()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);

        // add three pages
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        assertBufferInfo(buffer, 3, 0);

        // get the three pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 3, 0);

        // get the three pages again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 3, 0);

        // acknowledge the pages
        buffer.getPages(3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again, which will return an empty resilt
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 0, 3);
    }

    @Test
    public void testAddAfterNoMorePages()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertBufferInfo(buffer, 0, 0);
    }

    @Test
    public void testAddAfterDestroy()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertBufferDestroyed(buffer, 0);
    }

    @Test
    public void testDestroy()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);

        // add 5 pages the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();

        // read a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));

        // destroy without acknowledgement
        buffer.destroy();
        assertBufferDestroyed(buffer, 0);

        // follow token from previous read, which should return a finished result
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testNoMorePagesFreesReader()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.getPages(0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getPages(1, sizeOfPages(10));
        assertFalse(future.isDone());

        // finish the buffer
        buffer.setNoMorePages();
        assertBufferInfo(buffer, 0, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testDestroyFreesReader()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.getPages(0, sizeOfPages(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));
        assertTrue(future.isDone());

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getPages(1, sizeOfPages(10));
        assertFalse(future.isDone());

        // destroy the buffer
        buffer.destroy();

        // verify the future completed
        // buffer does not return a "complete" result in this case, but it doesn't matter
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));

        // further requests will see a completed result
        assertBufferDestroyed(buffer, 1);
    }

    @Test
    public void testInvalidTokenFails()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, NOOP_RELEASE_LISTENER);
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        buffer.getPages(1, sizeOfPages(10)).cancel(true);
        assertBufferInfo(buffer, 1, 1);

        // request negative token
        assertInvalidSequenceId(buffer, -1);
        assertBufferInfo(buffer, 1, 1);

        // request token off end of buffer
        assertInvalidSequenceId(buffer, 10);
        assertBufferInfo(buffer, 1, 1);
    }

    @Test
    public void testReferenceCount()
    {
        AtomicInteger releasedPages = new AtomicInteger(0);
        PagesReleasedListener onPagesReleased = (lifespan, releasedPagesCount, releasedSizeInBytes) -> {
            releasedPages.addAndGet(releasedPagesCount);
        };
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID, onPagesReleased);

        // add 2 pages and verify they are referenced
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertEquals(releasedPages.get(), 0);
        assertBufferInfo(buffer, 2, 0);

        // read one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(0), NO_WAIT), bufferResult(0, createPage(0)));
        assertEquals(releasedPages.get(), 0);
        assertBufferInfo(buffer, 2, 0);

        // acknowledge first page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 1, sizeOfPages(1), NO_WAIT), bufferResult(1, createPage(1)));
        assertEquals(releasedPages.get(), 1);
        assertBufferInfo(buffer, 1, 1);

        // destroy the buffer
        buffer.destroy();
        assertEquals(releasedPages.get(), 2);
        assertBufferDestroyed(buffer, 1);
    }

    private static void assertInvalidSequenceId(ClientBuffer buffer, int sequenceId)
    {
        try {
            buffer.getPages(sequenceId, sizeOfPages(10));
            fail("Expected " + INVALID_SEQUENCE_ID);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), INVALID_SEQUENCE_ID);
        }
    }

    private static BufferResult getBufferResult(ClientBuffer buffer, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.getPages(sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    private static BufferResult getBufferResult(ClientBuffer buffer, PagesSupplier supplier, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.getPages(sequenceId, maxSize, Optional.of(supplier));
        return getFuture(future, maxWait);
    }

    private static void addPage(ClientBuffer buffer, Page page)
    {
        addPage(buffer, page, NOOP_RELEASE_LISTENER);
    }

    private static void addPage(ClientBuffer buffer, Page page, PagesReleasedListener onPagesReleased)
    {
        SerializedPageReference serializedPageReference = new SerializedPageReference(PAGES_SERDE.serialize(page), 1, Lifespan.taskWide());
        buffer.enqueuePages(ImmutableList.of(serializedPageReference));
        dereferencePages(ImmutableList.of(serializedPageReference), onPagesReleased);
    }

    private static void assertBufferInfo(
            ClientBuffer buffer,
            int bufferedPages,
            int pagesSent)
    {
        assertEquals(
                buffer.getInfo(),
                new BufferInfo(
                        BUFFER_ID,
                        false,
                        bufferedPages,
                        pagesSent,
                        new PageBufferInfo(
                                BUFFER_ID.getId(),
                                bufferedPages,
                                sizeOfPages(bufferedPages).toBytes(),
                                bufferedPages + pagesSent, // every page has one row
                                bufferedPages + pagesSent)));
        assertFalse(buffer.isDestroyed());
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertBufferDestroyed(ClientBuffer buffer, int pagesSent)
    {
        BufferInfo bufferInfo = buffer.getInfo();
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertTrue(bufferInfo.isFinished());
        assertTrue(buffer.isDestroyed());
    }

    @ThreadSafe
    private static class TestingPagesSupplier
            implements PagesSupplier
    {
        @GuardedBy("this")
        private final Deque<SerializedPageReference> buffer = new ArrayDeque<>();

        @GuardedBy("this")
        private boolean noMorePages;

        @Override
        public synchronized boolean mayHaveMorePages()
        {
            return !noMorePages || !buffer.isEmpty();
        }

        synchronized void setNoMorePages()
        {
            this.noMorePages = true;
        }

        synchronized int getBufferedPages()
        {
            return buffer.size();
        }

        public synchronized void addPage(Page page)
        {
            requireNonNull(page, "page is null");
            checkState(!noMorePages);
            buffer.add(new SerializedPageReference(PAGES_SERDE.serialize(page), 1, Lifespan.taskWide()));
        }

        @Override
        public synchronized List<SerializedPageReference> getPages(DataSize maxSize)
        {
            long maxBytes = maxSize.toBytes();
            List<SerializedPageReference> pages = new ArrayList<>();
            long bytesRemoved = 0;

            while (true) {
                SerializedPageReference page = buffer.peek();
                if (page == null) {
                    break;
                }
                bytesRemoved += page.getRetainedSizeInBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                    break;
                }
                // this should not happen since we have a lock
                checkState(buffer.poll() == page, "Buffer corrupted");
                pages.add(page);
            }

            return ImmutableList.copyOf(pages);
        }
    }
}
