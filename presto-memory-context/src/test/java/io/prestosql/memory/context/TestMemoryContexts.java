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
package io.prestosql.memory.context;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestMemoryContexts
{
    private static final ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);
    private static final long GUARANTEED_MEMORY = new DataSize(1, MEGABYTE).toBytes();

    @Test
    public void testLocalMemoryContextClose()
            throws IOException
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext aggregateContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");
        localContext.setBytes(100);

        assertEquals(localContext.getBytes(), 100);
        assertEquals(aggregateContext.getBytes(), 100);
        assertEquals(reservationHandler.getReservation(), 100);

        localContext.close();
        assertEquals(localContext.getBytes(), 0);
        assertEquals(aggregateContext.getBytes(), 0);
        assertEquals(reservationHandler.getReservation(), 0);
    }

    @Test
    public void testMemoryContexts()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext aggregateContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");

        assertEquals(localContext.setBytes(10), NOT_BLOCKED);
        assertEquals(localContext.getBytes(), 10);
        assertEquals(aggregateContext.getBytes(), 10);
        assertEquals(reservationHandler.getReservation(), aggregateContext.getBytes());

        LocalMemoryContext secondLocalContext = aggregateContext.newLocalMemoryContext("test");
        assertEquals(secondLocalContext.setBytes(20), NOT_BLOCKED);
        assertEquals(secondLocalContext.getBytes(), 20);
        assertEquals(aggregateContext.getBytes(), 30);
        assertEquals(localContext.getBytes(), 10);
        assertEquals(reservationHandler.getReservation(), aggregateContext.getBytes());

        aggregateContext.close();

        assertEquals(aggregateContext.getBytes(), 0);
        assertEquals(reservationHandler.getReservation(), 0);
    }

    @Test
    public void testTryReserve()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext parentContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        AggregatedMemoryContext aggregateContext1 = parentContext.newAggregatedMemoryContext();
        AggregatedMemoryContext aggregateContext2 = parentContext.newAggregatedMemoryContext();
        LocalMemoryContext childContext1 = aggregateContext1.newLocalMemoryContext("test");

        assertTrue(childContext1.trySetBytes(500));
        assertTrue(childContext1.trySetBytes(1_000));
        assertFalse(childContext1.trySetBytes(1_001));
        assertEquals(reservationHandler.getReservation(), parentContext.getBytes());

        aggregateContext1.close();
        aggregateContext2.close();
        parentContext.close();

        assertEquals(aggregateContext1.getBytes(), 0);
        assertEquals(aggregateContext2.getBytes(), 0);
        assertEquals(parentContext.getBytes(), 0);
        assertEquals(reservationHandler.getReservation(), 0);
    }

    @Test
    public void testHieararchicalMemoryContexts()
    {
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(1_000);
        AggregatedMemoryContext parentContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        AggregatedMemoryContext aggregateContext1 = parentContext.newAggregatedMemoryContext();
        AggregatedMemoryContext aggregateContext2 = parentContext.newAggregatedMemoryContext();
        LocalMemoryContext childContext1 = aggregateContext1.newLocalMemoryContext("test");
        LocalMemoryContext childContext2 = aggregateContext2.newLocalMemoryContext("test");

        assertEquals(childContext1.setBytes(1), NOT_BLOCKED);
        assertEquals(childContext2.setBytes(1), NOT_BLOCKED);

        assertEquals(aggregateContext1.getBytes(), 1);
        assertEquals(aggregateContext2.getBytes(), 1);
        assertEquals(parentContext.getBytes(), aggregateContext1.getBytes() + aggregateContext2.getBytes());
        assertEquals(reservationHandler.getReservation(), parentContext.getBytes());
    }

    @Test
    public void testGuaranteedMemoryDoesntBlock()
    {
        long maxMemory = 2 * GUARANTEED_MEMORY;
        TestMemoryReservationHandler reservationHandler = new TestMemoryReservationHandler(maxMemory);
        AggregatedMemoryContext parentContext = newRootAggregatedMemoryContext(reservationHandler, GUARANTEED_MEMORY);
        LocalMemoryContext childContext = parentContext.newLocalMemoryContext("test");

        // exhaust the max memory available
        reservationHandler.exhaustMemory();
        assertEquals(reservationHandler.getReservation(), maxMemory);

        // even if the pool is exhausted we never block queries using a trivial amount of memory
        assertEquals(childContext.setBytes(1_000), NOT_BLOCKED);
        assertNotEquals(childContext.setBytes(GUARANTEED_MEMORY + 1), NOT_BLOCKED);

        // at this point the memory contexts have reserved GUARANTEED_MEMORY + 1 bytes
        childContext.close();
        parentContext.close();

        assertEquals(childContext.getBytes(), 0);
        assertEquals(parentContext.getBytes(), 0);

        // since we have exhausted the memory above after the memory contexts are closed
        // the pool must still be exhausted
        assertEquals(reservationHandler.getReservation(), maxMemory);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "SimpleLocalMemoryContext is already closed")
    public void testClosedLocalMemoryContext()
    {
        AggregatedMemoryContext aggregateContext = newSimpleAggregatedMemoryContext();
        LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");
        localContext.close();
        localContext.setBytes(100);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "SimpleAggregatedMemoryContext is already closed")
    public void testClosedAggregateMemoryContext()
    {
        AggregatedMemoryContext aggregateContext = newSimpleAggregatedMemoryContext();
        LocalMemoryContext localContext = aggregateContext.newLocalMemoryContext("test");
        aggregateContext.close();
        localContext.setBytes(100);
    }

    private static class TestMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private long reservation;
        private final long maxMemory;
        private SettableFuture<?> future;

        public TestMemoryReservationHandler(long maxMemory)
        {
            this.maxMemory = maxMemory;
        }

        public long getReservation()
        {
            return reservation;
        }

        @Override
        public ListenableFuture<?> reserveMemory(String allocationTag, long delta)
        {
            reservation += delta;
            if (delta >= 0) {
                if (reservation >= maxMemory) {
                    future = SettableFuture.create();
                    return future;
                }
            }
            else {
                if (reservation < maxMemory) {
                    if (future != null) {
                        future.set(null);
                    }
                }
            }
            return NOT_BLOCKED;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            if (reservation + delta > maxMemory) {
                return false;
            }
            reservation += delta;
            return true;
        }

        public void exhaustMemory()
        {
            reservation = maxMemory;
        }
    }
}
