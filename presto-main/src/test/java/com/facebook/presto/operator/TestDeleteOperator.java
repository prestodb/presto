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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestDeleteOperator
{
    private static final List<Type> TYPES = ImmutableList.<Type>of(VARCHAR);
    private static final Page PAGE = createSequencePage(TYPES, 10, 100);

    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    private DriverContext getDriverContext()
    {
        return createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @Test
    public void testDelete()
            throws Exception
    {
        DriverContext driverContext = getDriverContext();

        DeleteOperator.DeleteOperatorFactory operatorFactory = new DeleteOperator.DeleteOperatorFactory(0, 0);

        Operator operator = operatorFactory.createOperator(driverContext);

        if (operator instanceof DeleteOperator) {
            ((DeleteOperator) operator).setPageSource(() -> Optional.of(new MockUpdatablePageSource()));
        }

        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);

        operator.addInput(PAGE);
        long systemMemoryAfterAddInput = operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes();
        assertTrue(systemMemoryAfterAddInput > 0);

        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);
        assertNull(operator.getOutput());

        operator.finish();
        Page page = operator.getOutput();
        assertNotNull(page);
        assertEquals(page.getPositionCount(), 1);
        assertTrue(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes() >= systemMemoryAfterAddInput);

        // sleep for a bit to make sure that there aren't extra pages on the way
        Thread.sleep(10);

        operator.close();

        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertNull(operator.getOutput());
        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);
    }

    private class MockUpdatablePageSource
             implements UpdatablePageSource
    {
        @Override
        public void deleteRows(Block rowIds)
        {
        }

        @Override
        public Collection<Slice> commit()
        {
            return ImmutableList.of();
        }

        @Override
        public long getDeltaMemory()
        {
            return 120;
        }

        @Override
        public Page getNextPage()
        {
            return null;
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }
    }
}
