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
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.BufferTestUtils.MAX_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.NO_WAIT;
import static com.facebook.presto.execution.buffer.BufferTestUtils.acknowledgeBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.addPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.addPages;
import static com.facebook.presto.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.createPage;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getBufferResult;
import static com.facebook.presto.execution.buffer.BufferTestUtils.getFuture;
import static com.facebook.presto.execution.buffer.BufferTestUtils.sizeOfPages;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.SPOOLING;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSpoolingOutputBuffer
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final DataSize THRESHOLD = sizeOfPages(3);
    private static final List<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId BUFFER_ID = new OutputBufferId(0);
    private static final OutputBufferId INVALID_BUFFER_ID = new OutputBufferId(1);
    private static final OutputBuffers OUTPUT_BUFFERS = OutputBuffers.createSpoolingOutputBuffers();

    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    private static SpoolingOutputBufferFactory spoolingOutputBufferFactory;

    private ScheduledExecutorService stateNotificationExecutor;

    @BeforeClass
    public void setUp()
    {
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-%s"));

        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpoolingOutputBufferThreshold(THRESHOLD);
        spoolingOutputBufferFactory = new SpoolingOutputBufferFactory(featuresConfig);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
        spoolingOutputBufferFactory.shutdown();
    }

    @Test
    public void testSimpleInMemory()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        // add three pages
        for (int i = 0; i < 2; i++) {
            addPage(buffer, createPage(i));
        }
        compareTotalBuffered(buffer, 2);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        compareTotalBuffered(buffer, 2);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 1, sizeOfPages(1), MAX_WAIT), bufferResult(1, createPage(1)));
        compareTotalBuffered(buffer, 1);

        buffer.setNoMorePages();
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 2, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 2, true));
        compareTotalBuffered(buffer, 0);
    }

    @Test
    public void testSimple()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        List<Page> pages = new LinkedList<>();
        // add five pages to storage
        for (int i = 0; i < 5; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);
        compareTotalBuffered(buffer, 5);

        // get one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        compareTotalBuffered(buffer, 5);

        // get two more pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 1, sizeOfPages(2), MAX_WAIT), bufferResult(1, createPage(1), createPage(2)));
        compareTotalBuffered(buffer, 5);

        // add three more pages to storage
        pages = new LinkedList<>();
        for (int i = 5; i < 8; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);
        compareTotalBuffered(buffer, 8);

        // get three pages from both first and second file
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 3, sizeOfPages(3), MAX_WAIT), bufferResult(3, createPage(3), createPage(4), createPage(5)));
        compareTotalBuffered(buffer, 8);

        // get the last three pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 6, sizeOfPages(2), MAX_WAIT), bufferResult(6, createPage(6), createPage(7)));

        // first file removed, but second file remains
        compareTotalBuffered(buffer, 3);

        // add one page in memory
        addPage(buffer, createPage(8));

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 8, sizeOfPages(2), MAX_WAIT), bufferResult(8, createPage(8)));

        // second file removed from storage
        compareTotalBuffered(buffer, 1);

        ListenableFuture<BufferResult> pendingRead = buffer.get(BUFFER_ID, 9, sizeOfPages(1));
        assertFalse(pendingRead.isDone());

        // in memory page removed
        compareTotalBuffered(buffer, 0);

        buffer.setNoMorePages();

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 9, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 9, true));

        assertEquals(buffer.getInfo().getTotalPagesSent(), 9);
    }

    @Test
    void testUnevenMaxSize()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        List<Page> pages = new LinkedList<>();
        // add five pages to storage
        for (int i = 0; i < 5; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);

        // add three pages to memory
        for (int i = 5; i < 8; i++) {
            addPage(buffer, createPage(i));
        }

        DataSize unevenMaxSize = new DataSize(sizeOfPages(3).toBytes() + 5, BYTE);

        // should not try to read the in memory pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, unevenMaxSize, MAX_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        compareTotalBuffered(buffer, 8);
    }

    @Test
    void testGetOutOfOrder()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        List<Page> pages = new LinkedList<>();
        // add five pages to storage
        for (int i = 0; i < 5; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(3), MAX_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        compareTotalBuffered(buffer, 5);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 3, sizeOfPages(3), MAX_WAIT), bufferResult(3, createPage(3), createPage(4)));
        compareTotalBuffered(buffer, 5);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 2, sizeOfPages(3), MAX_WAIT), emptyResults(TASK_INSTANCE_ID, 2, false));
        compareTotalBuffered(buffer, 5);
    }

    @Test
    public void testSimplePendingRead()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(BUFFER_ID, 0, sizeOfPages(2));
        assertFalse(future.isDone());

        // add three pages
        List<Page> pages = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);

        // pending read should have two pages added
        assertBufferResultEquals(TYPES, getFuture(future, MAX_WAIT), bufferResult(0, createPage(0), createPage(1)));

        // checks we can still read first three pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(3), MAX_WAIT), createBufferResult(TASK_INSTANCE_ID, 0, pages));

        acknowledgeBufferResult(buffer, BUFFER_ID, 2);
        compareTotalBuffered(buffer, 3);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 2, sizeOfPages(3), MAX_WAIT), bufferResult(2, createPage(2)));

        // file should be removed after acknowledging all three pages
        acknowledgeBufferResult(buffer, BUFFER_ID, 3);
        compareTotalBuffered(buffer, 0);

        // attempt to read, but nothing can be read
        future = buffer.get(BUFFER_ID, 3, sizeOfPages(3));
        assertFalse(future.isDone());
    }

    @Test
    public void testMultiplePendingReads()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        // attempt to get a page
        ListenableFuture<BufferResult> oldPendingRead = buffer.get(BUFFER_ID, 0, sizeOfPages(3));
        assertFalse(oldPendingRead.isDone());

        ListenableFuture<BufferResult> newPendingRead = buffer.get(BUFFER_ID, 0, sizeOfPages(3));
        assertFalse(newPendingRead.isDone());
        assertTrue(oldPendingRead.isDone());

        // add three pages
        List<Page> pages = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);

        assertBufferResultEquals(TYPES, getFuture(oldPendingRead, MAX_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        assertBufferResultEquals(TYPES, getFuture(newPendingRead, MAX_WAIT), createBufferResult(TASK_INSTANCE_ID, 0, pages));
    }

    @Test
    public void testAddAfterPendingRead()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        ListenableFuture<BufferResult> pendingRead = buffer.get(BUFFER_ID, 0, sizeOfPages(5));
        assertFalse(pendingRead.isDone());

        List<Page> pages = new LinkedList<>();
        // add five pages to storage
        for (int i = 0; i < 5; i++) {
            pages.add(createPage(i));
        }
        addPages(buffer, pages);
        compareTotalBuffered(buffer, 5);

        assertBufferResultEquals(TYPES, getFuture(pendingRead, MAX_WAIT), createBufferResult(TASK_INSTANCE_ID, 0, pages));

        compareTotalBuffered(buffer, 5);

        acknowledgeBufferResult(buffer, BUFFER_ID, 5);

        buffer.setNoMorePages();

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 5, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 5, true));
    }

    @Test
    public void testNoMorePagesAfterPendingRead()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        ListenableFuture<BufferResult> pendingRead = buffer.get(BUFFER_ID, 0, sizeOfPages(5));
        assertFalse(pendingRead.isDone());

        buffer.setNoMorePages();

        assertBufferResultEquals(TYPES, getFuture(pendingRead, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testDestroyAfterPendingRead()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        ListenableFuture<BufferResult> pendingRead = buffer.get(BUFFER_ID, 0, sizeOfPages(5));
        assertFalse(pendingRead.isDone());

        buffer.destroy();

        assertTrue(pendingRead.isDone());
        assertBufferResultEquals(TYPES, getFuture(pendingRead, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
    }

    @Test
    public void testAcknowledgeSimple()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // get the three elements from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(10), MAX_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));

        // acknowledge pages 0 and 1
        acknowledgeBufferResult(buffer, BUFFER_ID, 2);
        compareTotalBuffered(buffer, 3);

        // acknowledge page 2
        acknowledgeBufferResult(buffer, BUFFER_ID, 3);
        compareTotalBuffered(buffer, 0);

        // acknowledge more pages will fail
        try {
            acknowledgeBufferResult(buffer, BUFFER_ID, 4);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid sequenceId");
        }
        compareTotalBuffered(buffer, 0);

        // fill the buffer
        for (int i = 3; i < 6; i++) {
            addPage(buffer, createPage(i));
        }

        // acknowledge previously acknowledged pages does nothing
        acknowledgeBufferResult(buffer, BUFFER_ID, 3);
        compareTotalBuffered(buffer, 3);
    }

    @Test
    public void testAcknowledgeStorageAndMemory()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        // add three pages to file
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages in memory
        for (int i = 3; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // remove file and one page from memory
        acknowledgeBufferResult(buffer, BUFFER_ID, 4);
        compareTotalBuffered(buffer, 1);
    }

    @Test
    public void testDuplicateGet()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        // add three pages
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(10), MAX_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        compareTotalBuffered(buffer, 3);

        // get the same three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(10), MAX_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        compareTotalBuffered(buffer, 3);

        // acknowledge the 3 pages
        acknowledgeBufferResult(buffer, BUFFER_ID, 3);
        compareTotalBuffered(buffer, 0);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        compareTotalBuffered(buffer, 0);
    }

    @Test
    public void testAddAfterNoMorePages()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        for (int i = 0; i < 2; i++) {
            addPage(buffer, createPage(i));
        }
        compareTotalBuffered(buffer, 2);

        buffer.setNoMorePages();

        // should not be added
        addPage(buffer, createPage(2));
        compareTotalBuffered(buffer, 2);

        // get the two pages added
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(3), MAX_WAIT), bufferResult(0, createPage(0), createPage(1)));

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 2, sizeOfPages(3), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 2, true));
    }

    @Test
    public void testAddAfterDestroy()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        for (int i = 0; i < 2; i++) {
            addPage(buffer, createPage(i));
        }
        compareTotalBuffered(buffer, 2);

        buffer.destroy();

        // nothing in buffer
        compareTotalBuffered(buffer, 0);

        // should not be added
        addPage(buffer, createPage(2));
        compareTotalBuffered(buffer, 0);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(3), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testAbort()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        // add three pages into a file
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add two page in memory
        for (int i = 3; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        try {
            buffer.abort(INVALID_BUFFER_ID);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid bufferId");
        }
        compareTotalBuffered(buffer, 5);

        buffer.abort(BUFFER_ID);
        compareTotalBuffered(buffer, 0);
    }

    @Test
    public void testSetOutputBuffers()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        OutputBuffers newBuffers = new OutputBuffers(SPOOLING, 1, true, ImmutableMap.of());
        buffer.setOutputBuffers(newBuffers);

        OutputBuffers invalidBuffers = new OutputBuffers(PARTITIONED, 1, true, ImmutableMap.of());
        try {
            buffer.setOutputBuffers(invalidBuffers);
        }
        catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Invalid output buffers type");
        }
    }

    @Test
    public void testBufferCompletion()
    {
        SpoolingOutputBuffer buffer = createSpoolingOutputBuffer();

        assertFalse(buffer.isFinished());

        // add three pages into a file
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Page page = createPage(i);
            addPage(buffer, page);
            pages.add(createPage(i));
        }

        // add two page in memory
        for (int i = 3; i < 5; i++) {
            Page page = createPage(i);
            addPage(buffer, page);
            pages.add(createPage(i));
        }
        compareTotalBuffered(buffer, 5);

        buffer.setNoMorePages();

        // get and acknowledge 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, BUFFER_ID, 0, sizeOfPages(5), MAX_WAIT), createBufferResult(TASK_INSTANCE_ID, 0, pages));

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertFalse(buffer.isFinished());

        // ask the buffer to finish
        buffer.destroy();

        // verify that the buffer is finished
        assertTrue(buffer.isFinished());
    }

    private SpoolingOutputBuffer createSpoolingOutputBuffer()
    {
        TaskId taskId = new TaskId(queryIdGenerator.createNextQueryId().toString(), 0, 0, 0);
        return spoolingOutputBufferFactory.createSpoolingOutputBuffer(
                taskId,
                TASK_INSTANCE_ID,
                OUTPUT_BUFFERS,
                new StateMachine<>("bufferState", stateNotificationExecutor, OPEN, TERMINAL_BUFFER_STATES));
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }

    private void compareTotalBuffered(OutputBuffer buffer, int expectedBufferedPages)
    {
        assertEquals(buffer.getInfo().getTotalBufferedBytes(), (int) sizeOfPages(expectedBufferedPages).getValue());
        assertEquals(buffer.getInfo().getTotalBufferedPages(), expectedBufferedPages);
    }
}
