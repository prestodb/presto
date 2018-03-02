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

import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.ClientBuffer.PagesSupplier;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferSummary.emptySummary;
import static com.facebook.presto.execution.buffer.BufferTestUtils.NO_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.PAGES_SERDE;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferSummaryEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferSummary;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getFuture;
import static com.facebook.presto.execution.buffer.BufferTestUtils.sizeOfPagesInBytes;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
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
    private static final String MORE_FETCH_THAN_PEEK = "Read more pages than peeked";

    @Test
    public void testSimplePushBuffer()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // add three pages to the buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        assertBufferInfo(buffer, 3, 0);

        // peek the pages elements from the buffer
        assertBufferSummaryEquals(getBufferSummary(buffer, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        // get the pages elements from the buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 3, 0);

        // acknowledge first three pages in the buffer
        buffer.getSummary(3, sizeOfPagesInBytes(10)).cancel(true);
        // pages now acknowledged
        assertBufferInfo(buffer, 0, 3);

        // add 3 more pages
        for (int i = 3; i < 6; i++) {
            addPage(buffer, createPage(i));
        }
        assertBufferInfo(buffer, 3, 3);

        // peek the pages elements from the buffer
        assertBufferSummaryEquals(getBufferSummary(buffer, 3, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(3, createPage(3)));
        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 3, sizeOfPagesInBytes(1)), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertBufferInfo(buffer, 3, 3);

        // set no more pages
        buffer.setNoMorePages();
        // state should not change
        assertBufferInfo(buffer, 3, 3);

        // peek the pages elements from the buffer
        assertBufferSummaryEquals(getBufferSummary(buffer, 4, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(4, createPage(4)));
        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 4, sizeOfPagesInBytes(1)), bufferResult(4, createPage(4)));
        assertBufferInfo(buffer, 2, 4);

        // peek the pages elements from the buffer
        assertBufferSummaryEquals(getBufferSummary(buffer, 5, sizeOfPagesInBytes(30), NO_WAIT), bufferSummary(5, createPage(5)));
        // remove last pages from, should not be finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 5, sizeOfPagesInBytes(30)), bufferResult(5, createPage(5)));
        assertBufferInfo(buffer, 1, 5);

        // acknowledge all pages from the buffer, should return a finished buffer result
        assertBufferSummaryEquals(getBufferSummary(buffer, 6, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 6, true));
        assertBufferInfo(buffer, 0, 6);

        // buffer is not destroyed until explicitly destroyed
        buffer.destroy();
        assertBufferDestroyed(buffer, 6);
    }

    @Test
    public void testSimplePullBuffer()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // create a page supplier with 3 initial pages
        TestingPagesSupplier supplier = new TestingPagesSupplier();
        for (int i = 0; i < 3; i++) {
            supplier.addPage(createPage(i));
        }
        assertEquals(supplier.getBufferedPages(), 3);

        // get the pages elements from the buffer
        assertBufferSummaryEquals(getBufferSummary(buffer, supplier, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        // 3 pages are removed from the supplier
        assertEquals(supplier.getBufferedPages(), 0);

        // get the pages elements from the buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // 3 pages are moved to the client buffer, but not acknowledged yet
        assertBufferInfo(buffer, 3, 0);

        // acknowledge first three pages in the buffer
        ListenableFuture<BufferSummary> pendingRead = buffer.getSummary(3, sizeOfPagesInBytes(1));
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
        assertBufferSummaryEquals(getFuture(pendingRead, NO_WAIT), bufferSummary(3, createPage(3)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 3, sizeOfPagesInBytes(1)), bufferResult(3, createPage(3)));
        // 1 page wad moved to the client buffer, but not acknowledged yet
        assertEquals(supplier.getBufferedPages(), 2);
        assertBufferInfo(buffer, 1, 3);

        // set no more pages
        supplier.setNoMorePages();
        // state should not change
        assertEquals(supplier.getBufferedPages(), 2);
        assertBufferInfo(buffer, 1, 3);

        // remove a page
        assertBufferSummaryEquals(getBufferSummary(buffer, supplier, 4, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(4, createPage(4)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 4, sizeOfPagesInBytes(1)), bufferResult(4, createPage(4)));
        assertBufferInfo(buffer, 1, 4);
        assertEquals(supplier.getBufferedPages(), 1);

        // remove last pages from, should not be finished
        assertBufferSummaryEquals(getBufferSummary(buffer, supplier, 5, sizeOfPagesInBytes(30), NO_WAIT), bufferSummary(5, createPage(5)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 5, sizeOfPagesInBytes(30)), bufferResult(5, createPage(5)));
        assertBufferInfo(buffer, 1, 5);
        assertEquals(supplier.getBufferedPages(), 0);

        // acknowledge all pages from the buffer, should return a finished buffer result
        assertBufferSummaryEquals(getBufferSummary(buffer, supplier, 6, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 6, true));
        assertBufferInfo(buffer, 0, 6);
        assertEquals(supplier.getBufferedPages(), 0);

        // buffer is not destroyed until explicitly destroyed
        buffer.destroy();
        assertBufferDestroyed(buffer, 6);
    }

    @Test
    public void testDuplicateRequests()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // add three pages
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        assertBufferInfo(buffer, 3, 0);

        // get the three pages
        assertBufferSummaryEquals(getBufferSummary(buffer, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 3, 0);

        // get the three pages again
        assertBufferSummaryEquals(getBufferSummary(buffer, 0, sizeOfPagesInBytes(10), NO_WAIT), bufferSummary(0, createPage(0), createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 3, 0);

        // acknowledge the pages
        buffer.getSummary(3, sizeOfPagesInBytes(10)).cancel(true);

        // attempt to get the three elements again, which will return an empty result
        assertBufferSummaryEquals(getBufferSummary(buffer, 0, sizeOfPagesInBytes(10), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 0, false));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(10)), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages not acknowledged yet so state is the same
        assertBufferInfo(buffer, 0, 3);
    }

    @Test
    public void testAddAfterNoMorePages()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertBufferInfo(buffer, 0, 0);
    }

    @Test
    public void testAddAfterDestroy()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertBufferDestroyed(buffer, 0);
    }

    @Test
    public void testDestroy()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // add 5 pages the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();

        // read a page
        assertBufferSummaryEquals(getBufferSummary(buffer, 0, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(1)), bufferResult(0, createPage(0)));

        // destroy without acknowledgement
        buffer.destroy();
        assertBufferDestroyed(buffer, 0);

        // follow token from previous read, which should return a finished result
        assertBufferSummaryEquals(getBufferSummary(buffer, 1, sizeOfPagesInBytes(1), NO_WAIT), emptySummary(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testNoMorePagesFreesReader()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // attempt to peek a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(0)));

        // verify we can get the page
        assertBufferResultEquals(TYPES, buffer.getData(0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // finish the buffer
        buffer.setNoMorePages();
        assertBufferInfo(buffer, 0, 1);

        // verify the future completed
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), emptySummary(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testDestroyFreesReader()
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // attempt to peek a page
        ListenableFuture<BufferSummary> future = buffer.getSummary(0, sizeOfPagesInBytes(10));

        // verify we are waiting for a page
        assertFalse(future.isDone());

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), bufferSummary(0, createPage(0)));

        // verify we can get the page
        assertBufferResultEquals(TYPES, buffer.getData(0, sizeOfPagesInBytes(10)), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.getSummary(1, sizeOfPagesInBytes(10));
        assertFalse(future.isDone());

        // destroy the buffer
        buffer.destroy();

        // verify the future completed
        // buffer does not return a "complete" result in this case, but it doesn't matter
        assertBufferSummaryEquals(getFuture(future, NO_WAIT), emptySummary(TASK_INSTANCE_ID, 1, false));

        // further requests will see a completed result
        assertBufferDestroyed(buffer, 1);
    }

    @Test
    public void testInvalidTokenFails()
    {
        ClientBuffer buffer = clientBuffer(2);
        buffer.getSummary(0, sizeOfPagesInBytes(10)).cancel(true);
        buffer.getData(0, sizeOfPagesInBytes(10));
        buffer.getSummary(1, sizeOfPagesInBytes(1)).cancel(true);
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
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // add 2 pages and verify they are referenced
        AtomicBoolean page0HasReference = addPage(buffer, createPage(0));
        AtomicBoolean page1HasReference = addPage(buffer, createPage(1));
        assertTrue(page0HasReference.get());
        assertTrue(page1HasReference.get());
        assertBufferInfo(buffer, 2, 0);

        // read one page
        assertBufferSummaryEquals(getBufferSummary(buffer, 0, sizeOfPagesInBytes(0), NO_WAIT), bufferSummary(0, createPage(0)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPagesInBytes(0)), bufferResult(0, createPage(0)));
        assertTrue(page0HasReference.get());
        assertTrue(page1HasReference.get());
        assertBufferInfo(buffer, 2, 0);

        // acknowledge first page
        assertBufferSummaryEquals(getBufferSummary(buffer, 1, sizeOfPagesInBytes(1), NO_WAIT), bufferSummary(1, createPage(1)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 1, sizeOfPagesInBytes(1)), bufferResult(1, createPage(1)));
        assertFalse(page0HasReference.get());
        assertTrue(page1HasReference.get());
        assertBufferInfo(buffer, 1, 1);

        // destroy the buffer
        buffer.destroy();
        assertFalse(page0HasReference.get());
        assertFalse(page1HasReference.get());
        assertBufferDestroyed(buffer, 1);
    }

    @Test
    public void testOutOfRangeGet()
            throws Exception
    {
        // fetching page directly without peeking
        ClientBuffer buffer = clientBuffer(5);
        assertInvalidFetch(buffer, 0, 1, MORE_FETCH_THAN_PEEK);

        // peeking ranges [0, 1), but fetching starts at 2
        buffer = clientBuffer(5);
        buffer.getSummary(0, sizeOfPagesInBytes(1)).cancel(true);
        assertInvalidFetch(buffer, 2, 1, MORE_FETCH_THAN_PEEK);

        // peeking ranges [0, 1), but fetching ranges [0, 10)
        buffer = clientBuffer(5);
        buffer.getSummary(0, sizeOfPagesInBytes(1)).cancel(true);
        assertInvalidFetch(buffer, 0, 10, MORE_FETCH_THAN_PEEK);
    }

    @Test
    public void testMultipleGets()
            throws Exception
    {
        // get page summary only once
        ClientBuffer buffer = clientBuffer(15);
        buffer.getSummary(0, sizeOfPagesInBytes(10)).cancel(true);

        // multiple fetches are allowed as long as they are within range
        assertBufferResultEquals(TYPES, buffer.getData(0, sizeOfPagesInBytes(3)), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, buffer.getData(3, sizeOfPagesInBytes(4)), bufferResult(3, createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, buffer.getData(7, sizeOfPagesInBytes(2)), bufferResult(7, createPage(7), createPage(8)));

        // fail to fetch if it goes out of range
        assertInvalidFetch(buffer, 9, 2, MORE_FETCH_THAN_PEEK);

        // within range ok
        assertBufferResultEquals(TYPES, buffer.getData(9, sizeOfPagesInBytes(1)), bufferResult(9, createPage(9)));

        // get page summary again; then we are able to fetch more
        buffer.getSummary(10, sizeOfPagesInBytes(5)).cancel(true);
        assertBufferResultEquals(TYPES, buffer.getData(10, sizeOfPagesInBytes(1)), bufferResult(10, createPage(10)));
        assertBufferResultEquals(TYPES, buffer.getData(11, sizeOfPagesInBytes(4)), bufferResult(11, createPage(11), createPage(12), createPage(13), createPage(14)));
    }

    private static ClientBuffer clientBuffer(int pageCount)
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);
        for (int i = 0; i < pageCount; i++) {
            addPage(buffer, createPage(i));
        }
        return buffer;
    }

    private static void assertInvalidSequenceId(ClientBuffer buffer, int sequenceId)
    {
        try {
            buffer.getSummary(sequenceId, sizeOfPagesInBytes(10));
            fail("Expected " + INVALID_SEQUENCE_ID);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), INVALID_SEQUENCE_ID);
        }
    }

    private static void assertInvalidFetch(ClientBuffer buffer, int sequenceId, int pageCount, String message)
    {
        try {
            buffer.getData(sequenceId, sizeOfPagesInBytes(pageCount));
            fail("Expected " + message);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), message);
        }
    }

    private static BufferSummary getBufferSummary(ClientBuffer buffer, long sequenceId, long maxBytes, Duration maxWait)
    {
        ListenableFuture<BufferSummary> future = buffer.getSummary(sequenceId, maxBytes);
        return getFuture(future, maxWait);
    }

    private static BufferSummary getBufferSummary(ClientBuffer buffer, PagesSupplier supplier, long sequenceId, long maxBytes, Duration maxWait)
    {
        ListenableFuture<BufferSummary> future = buffer.getSummary(sequenceId, maxBytes, Optional.of(supplier));
        return getFuture(future, maxWait);
    }

    private static BufferResult getBufferResult(ClientBuffer buffer, long sequenceId, long maxBytes)
    {
        return buffer.getData(sequenceId, maxBytes);
    }

    private static AtomicBoolean addPage(ClientBuffer buffer, Page page)
    {
        AtomicBoolean dereferenced = new AtomicBoolean(true);
        SerializedPageReference serializedPageReference = new SerializedPageReference(PAGES_SERDE.serialize(page), 1, () -> dereferenced.set(false));
        buffer.enqueuePages(ImmutableList.of(serializedPageReference));
        serializedPageReference.dereferencePage();
        return dereferenced;
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
                                sizeOfPagesInBytes(bufferedPages),
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

    private static BufferSummary bufferSummary(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferSummary(TASK_INSTANCE_ID, token, pages);
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
            buffer.add(new SerializedPageReference(PAGES_SERDE.serialize(page), 1, () -> {}));
        }

        @Override
        public synchronized List<SerializedPageReference> getPages(long maxBytes)
        {
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
