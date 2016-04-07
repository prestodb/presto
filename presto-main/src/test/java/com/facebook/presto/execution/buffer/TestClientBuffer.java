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
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.execution.buffer.ClientBuffer.PageReference;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestClientBuffer
{
    private static final Duration NO_WAIT = new Duration(0, MILLISECONDS);
    private static final DataSize PAGE_SIZE = new DataSize(createPage(42).getRetainedSizeInBytes(), BYTE);
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final ImmutableList<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId BUFFER_ID = new OutputBufferId(33);
    private static final String INVALID_SEQUENCE_ID = "Invalid sequence id";

    @Test
    public void testSimplePartitioned()
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

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
    public void testDuplicateRequests()
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

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
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertBufferInfo(buffer, 0, 0);
    }

    @Test
    public void testAddAfterDestroy()
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertBufferDestroyed(buffer, 0);
    }

    @Test
    public void testDestroy()
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

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
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // attempt to get a page
        CompletableFuture<BufferResult> future = buffer.getPages(0, sizeOfPages(10));

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
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // attempt to get a page
        CompletableFuture<BufferResult> future = buffer.getPages(0, sizeOfPages(10));

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
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);
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
            throws Exception
    {
        ClientBuffer buffer = new ClientBuffer(TASK_INSTANCE_ID, BUFFER_ID);

        // add 2 pages and verify they are referenced
        AtomicBoolean page0HasReference = addPage(buffer, createPage(0));
        AtomicBoolean page1HasReference = addPage(buffer, createPage(1));
        assertTrue(page0HasReference.get());
        assertTrue(page1HasReference.get());
        assertBufferInfo(buffer, 2, 0);

        // read one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 0, sizeOfPages(0), NO_WAIT), bufferResult(0, createPage(0)));
        assertTrue(page0HasReference.get());
        assertTrue(page1HasReference.get());
        assertBufferInfo(buffer, 2, 0);

        // acknowledge first page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, 1, sizeOfPages(1), NO_WAIT), bufferResult(1, createPage(1)));
        assertFalse(page0HasReference.get());
        assertTrue(page1HasReference.get());
        assertBufferInfo(buffer, 1, 1);

        // destroy the buffer
        buffer.destroy();
        assertFalse(page0HasReference.get());
        assertFalse(page1HasReference.get());
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
        CompletableFuture<BufferResult> future = buffer.getPages(sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    private static BufferResult getFuture(CompletableFuture<BufferResult> future, Duration maxWait)
    {
        return tryGetFutureValue(future, (int) maxWait.toMillis(), MILLISECONDS).get();
    }

    private static AtomicBoolean addPage(ClientBuffer buffer, Page page)
    {
        AtomicBoolean dereferenced = new AtomicBoolean(true);
        PageReference pageReference = new PageReference(page, 1, () -> dereferenced.set(false));
        buffer.enqueuePages(ImmutableList.of(pageReference));
        pageReference.dereferencePage();
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
                                sizeOfPages(bufferedPages).toBytes(),
                                bufferedPages + pagesSent, // every page has one row
                                bufferedPages + pagesSent)));
        assertEquals(buffer.isDestroyed(), false);
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertBufferDestroyed(ClientBuffer buffer, int pagesSent)
    {
        BufferInfo bufferInfo = buffer.getInfo();
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getPagesSent(), pagesSent);
        assertEquals(bufferInfo.isFinished(), true);
        assertEquals(buffer.isDestroyed(), true);
    }

    private static void assertBufferResultEquals(List<? extends Type> types, BufferResult actual, BufferResult expected)
    {
        assertEquals(actual.getPages().size(), expected.getPages().size());
        assertEquals(actual.getToken(), expected.getToken());
        for (int i = 0; i < actual.getPages().size(); i++) {
            Page actualPage = actual.getPages().get(i);
            Page expectedPage = expected.getPages().get(i);
            assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
            PageAssertions.assertPageEquals(types, actualPage, expectedPage);
        }
        assertEquals(actual.isBufferComplete(), expected.isBufferComplete());
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return bufferResult(token, pages);
    }

    private static BufferResult bufferResult(long token, List<Page> pages)
    {
        checkArgument(!pages.isEmpty(), "pages is empty");
        return new BufferResult(TASK_INSTANCE_ID, token, token + pages.size(), false, pages);
    }

    private static Page createPage(int i)
    {
        return new Page(BlockAssertions.createLongsBlock(i));
    }

    private static DataSize sizeOfPages(int count)
    {
        return new DataSize(PAGE_SIZE.toBytes() * count, BYTE);
    }
}
