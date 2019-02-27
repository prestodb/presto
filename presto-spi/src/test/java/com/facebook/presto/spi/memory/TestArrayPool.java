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
package com.facebook.presto.spi.memory;

import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.memory.Caches.BOOLEAN_LARGEST_ARRAY_SIZE;
import static com.facebook.presto.spi.memory.Caches.BOOLEAN_POOL_CAPACITY;
import static com.facebook.presto.spi.memory.Caches.BYTE_LARGEST_ARRAY_SIZE;
import static com.facebook.presto.spi.memory.Caches.BYTE_POOL_CAPACITY;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestArrayPool
{
    private static ArrayPool<boolean[]> booleanArrayPool = Caches.getBooleanArrayPool();
    private static ArrayPool<byte[]> byteArrayPool = Caches.getByteArrayPool();

    private static final Random RANDOM = new Random();
    private static final Logger LOGGER = Logger.get(TestArrayPool.class);

    @Test(threadPoolSize = 1)
    void TestSingleThreadAllocationWithinSizeLimit()
    {
        TestSingleThreadAllocationWithinSizeLimit(booleanArrayPool, BOOLEAN_POOL_CAPACITY, BOOLEAN_LARGEST_ARRAY_SIZE);
        TestSingleThreadAllocationWithinSizeLimit(byteArrayPool, BYTE_POOL_CAPACITY, BYTE_LARGEST_ARRAY_SIZE);
    }

    private void TestSingleThreadAllocationWithinSizeLimit(ArrayPool arrayPool, long poolCapacity, int largestObjectSizeInPool)
    {
        arrayPool.setCapacity(poolCapacity);

        for (int i = 0; i < 100; i++) {
            int requestedAllocationSize = new Random().nextInt(largestObjectSizeInPool);
            long expectedAllocationSizeInBytes = arrayPool.getAllocationSizeInBytes(requestedAllocationSize);

            Object allocatedBytes = arrayPool.allocate(requestedAllocationSize);
            assertNotNull(allocatedBytes);
            assertEquals(sizeOf(allocatedBytes), expectedAllocationSizeInBytes);
            assertTrue(arrayPool.getTotalSize() <= poolCapacity);

            arrayPool.release(allocatedBytes);
            assertTrue(arrayPool.getTotalSize() <= poolCapacity);
        }
    }

    @Test(threadPoolSize = 1)
    void TestSingleThreadAllocationBeyondSizeLimit()
    {
        TestSingleThreadAllocationBeyondSizeLimit(booleanArrayPool, BOOLEAN_POOL_CAPACITY, BOOLEAN_LARGEST_ARRAY_SIZE);
        TestSingleThreadAllocationBeyondSizeLimit(byteArrayPool, BYTE_POOL_CAPACITY, BYTE_LARGEST_ARRAY_SIZE);
    }

    void TestSingleThreadAllocationBeyondSizeLimit(ArrayPool arrayPool, long poolCapacity, int largestObjectSizeInPool)
    {
        // Choose a request size that is larger than the largestObjectSizeInPool in the pool
        final int requestedAllocationSize = largestObjectSizeInPool * 2;

        arrayPool.setCapacity(poolCapacity);

        long poolTotalSize = arrayPool.getTotalSize();
        assertTrue(poolTotalSize <= poolCapacity);

        for (int i = 0; i < 100; i++) {
            long expectedAllocationSizeInBytes = arrayPool.getAllocationSizeInBytes(requestedAllocationSize);

            Object allocatedBytes = arrayPool.allocate(requestedAllocationSize);
            assertNotNull(allocatedBytes);
            assertEquals(sizeOf(allocatedBytes), expectedAllocationSizeInBytes);
            assertEquals(arrayPool.getTotalSize(), poolTotalSize);

            // The allocatedBytes won't be added back to the pool because it's beyond size limit
            arrayPool.release(allocatedBytes);
            assertEquals(arrayPool.getTotalSize(), poolTotalSize);
            assertTrue(arrayPool.getTotalSize() <= poolCapacity);
        }
    }

    @Test(threadPoolSize = 1)
    void TestSingleThreadAllocationOfSameSize()
    {
        TestSingleThreadAllocationOfSameSize(booleanArrayPool, BOOLEAN_POOL_CAPACITY, BOOLEAN_LARGEST_ARRAY_SIZE);
        TestSingleThreadAllocationOfSameSize(byteArrayPool, BYTE_POOL_CAPACITY, BYTE_LARGEST_ARRAY_SIZE);
    }

    void TestSingleThreadAllocationOfSameSize(ArrayPool arrayPool, long poolCapacity, int largestObjectSizeInPool)
    {
        // Choose a size that is covered by the pool
        final int requestedAllocationSize = arrayPool.getAllocationSizeInBytes(largestObjectSizeInPool / 2);

        arrayPool.setCapacity(poolCapacity);

        // Allocate and release one object, so that there is at least one object with this size in the pool.
        Object allocatedBytes = arrayPool.allocate(requestedAllocationSize);
        assertNotNull(allocatedBytes);
        assertEquals(sizeOf(allocatedBytes), requestedAllocationSize);

        arrayPool.release(allocatedBytes);
        assertTrue(arrayPool.getTotalSize() >= requestedAllocationSize);

        long poolTotalSize = arrayPool.getTotalSize();
        assertTrue(arrayPool.getTotalSize() <= poolCapacity);

        for (int i = 0; i < 10; i++) {
            Object newAllocatedBytes = arrayPool.allocate(requestedAllocationSize);

            // The allocated array should be pre-allocated in the pool, so after the allocation the pool total size should be minus requestedAllocationSize
            assertNotNull(newAllocatedBytes);
            assertEquals(sizeOf(newAllocatedBytes), requestedAllocationSize);
            assertEquals(arrayPool.getTotalSize(), poolTotalSize - requestedAllocationSize);

            // The newAllocatedBytes will be added back to the pool
            arrayPool.release(newAllocatedBytes);
            assertEquals(arrayPool.getTotalSize(), poolTotalSize);
        }
    }

    @Test(threadPoolSize = 1)
    void TestSingleThreadAllocationsWithTrim()
    {
        // We will set the pool's capacity to a small number (The largest object size in the pool) to make sure we're reaching the capacity limit after one allocation request.
        TestSingleThreadAllocationsWithTrim(booleanArrayPool, BOOLEAN_LARGEST_ARRAY_SIZE, BOOLEAN_LARGEST_ARRAY_SIZE);
        TestSingleThreadAllocationsWithTrim(byteArrayPool, BYTE_LARGEST_ARRAY_SIZE, BYTE_LARGEST_ARRAY_SIZE);
    }

    void TestSingleThreadAllocationsWithTrim(ArrayPool arrayPool, long poolCapacity, int largestObjectSizeInPool)
    {
        arrayPool.setCapacity(poolCapacity);
        int requestedAllocationSize = largestObjectSizeInPool;

        // Allocate and release a buffer of the largestSize 64 * 1024, which will make the pool reaching its capacity afterwards.
        Object allocatedBytes = arrayPool.allocate(requestedAllocationSize);
        assertNotNull(allocatedBytes);
        assertEquals(sizeOf(allocatedBytes), requestedAllocationSize);
        assertTrue(arrayPool.getTotalSize() <= poolCapacity);

        arrayPool.release(allocatedBytes);
        assertTrue(arrayPool.getTotalSize() <= poolCapacity);

        // Allocate and release another buffer, which will trigger a trim. After the trimming, the object we added above shall be evicted.
        requestedAllocationSize = arrayPool.getAllocationSizeInBytes(largestObjectSizeInPool / 2);
        allocatedBytes = arrayPool.allocate(requestedAllocationSize);
        assertNotNull(allocatedBytes);
        assertEquals(sizeOf(allocatedBytes), requestedAllocationSize);
        assertTrue(arrayPool.getTotalSize() <= poolCapacity);

        arrayPool.release(allocatedBytes);
        assertTrue(arrayPool.getTotalSize() <= poolCapacity);
    }

    @Test
    void TestMultiThreadInCreasingRequests()
            throws Exception
    {
        TestMultiThreadsInCreasingRequests(booleanArrayPool);
        TestMultiThreadsInCreasingRequests(byteArrayPool);
        TestMultiThreadsDecreasingRequests(booleanArrayPool, BOOLEAN_LARGEST_ARRAY_SIZE);
        TestMultiThreadsDecreasingRequests(byteArrayPool, BYTE_LARGEST_ARRAY_SIZE);
    }

    void TestMultiThreadsInCreasingRequests(ArrayPool arrayPool)
            throws Exception
    {
        final int threadCount = 10;

        int[] objectSizesInPool = arrayPool.getArrayObjectSizes();

        // Issue multiple allocation requests in increasing disjoint range sizes.
        // Within each range the request size is random, but the allocated sizes for all requests would be the same largestRequestSize, i.e. the upperbound of that range
        // After each range is finished, the number of objects for this size is in the range of [1, threadCount].
        int smallestRequestSize = 1;
        int lastAverageObjectSize = arrayPool.getAverageObjectSize();
        for (int i = 0; i < objectSizesInPool.length; i++) {
            int largestRequestSize = objectSizesInPool[i];
            arrayPool.setCapacity(largestRequestSize * threadCount * 2);
            LOGGER.info("Conducting tests for request sizes in range [%d, %d].", smallestRequestSize, largestRequestSize);
            LOGGER.info("Pool capacity %d.", largestRequestSize * threadCount * 2);
            TestMultiThreadsInRange(arrayPool, smallestRequestSize, largestRequestSize, threadCount);

            int currentAverageObjectSize = arrayPool.getAverageObjectSize();
            LOGGER.info("Average object size after all requests are served is %d.", currentAverageObjectSize);

            // The currentAverageObjectSize must be greater than or equal to lastAverageObjectSize because the smaller objects are older
            // and would be trimmed earlier. The opposite doesn't hold if we do from larger range to smaller range, because evicting
            // older(could be smaller) objects could increase the average object size.
            assertTrue(currentAverageObjectSize >= lastAverageObjectSize, format(("currentAverageObjectSize %d, lastAverageObjectSize %d"), currentAverageObjectSize, lastAverageObjectSize));

            lastAverageObjectSize = currentAverageObjectSize;
            smallestRequestSize = largestRequestSize + 1;
        }
    }

    void TestMultiThreadsDecreasingRequests(ArrayPool arrayPool, int largestObjectSizeInPool)
            throws Exception
    {
        final int threadCount = 10;

        int[] objectSizesInPool = arrayPool.getArrayObjectSizes();

        // Issue multiple allocation requests in decreasing disjoint range sizes.
        // Within each range the request size is random, but the allocated sizes for all requests would be the same largestRequestSize, i.e. the upperbound of that range
        // After each range is finished, the number of objects for this size is in the range of [1, threadCount].
        int largestRequestSize = objectSizesInPool[objectSizesInPool.length - 1];
        int lastAverageObjectSize = largestObjectSizeInPool;
        for (int i = objectSizesInPool.length - 1; i > 0; i--) {
            int smallestRequestSize = objectSizesInPool[i - 1] + 1;
            arrayPool.setCapacity(largestRequestSize * threadCount * 2);
            LOGGER.info("Conducting tests for request sizes in range [%d, %d].", smallestRequestSize, largestRequestSize);
            LOGGER.info("Pool capacity %d.", largestRequestSize * threadCount * 2);
            TestMultiThreadsInRange(arrayPool, smallestRequestSize, largestRequestSize, threadCount);

            int currentAverageObjectSize = arrayPool.getAverageObjectSize();
            LOGGER.info("Average object size after all requests are served is %d.", currentAverageObjectSize);

            assertTrue(currentAverageObjectSize <= lastAverageObjectSize, format(("currentAverageObjectSize %d, lastAverageObjectSize %d"), currentAverageObjectSize, lastAverageObjectSize));

            lastAverageObjectSize = currentAverageObjectSize;
            largestRequestSize = smallestRequestSize - 1;
        }
    }

    private void TestMultiThreadsInRange(ArrayPool arrayPool, int low, int high, int threadCount)
            throws Exception
    {
        final int requestsPerThread = 3;
        //

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        Callable<Void> poolClient = () -> {
            try {
                for (int j = 0; j < requestsPerThread; j++) {
                    int requestedAllocationSize = low + RANDOM.nextInt(high - low);
                    assertTrue(requestedAllocationSize >= low && requestedAllocationSize <= high);
                    long expectedAllocationSizeInBytes = arrayPool.getAllocationSizeInBytes(requestedAllocationSize);

                    Object allocatedBytes = arrayPool.allocate(requestedAllocationSize);
                    assertNotNull(allocatedBytes);
                    assertEquals(sizeOf(allocatedBytes), expectedAllocationSizeInBytes);

                    Thread.sleep(RANDOM.nextInt(5));

                    arrayPool.release(allocatedBytes);
                }
                return null;
            }
            finally {
                latch.countDown();
            }
        };

        // Create a number of poolClients that keep submitting requests to the pool
        Future[] futures = new Future[threadCount];
        for (int i = 0; i < threadCount; i++) {
            futures[i] = executor.submit(poolClient);
        }

        // The main thread polls the status of the pool
        int lastAverageObjectSize = arrayPool.getAverageObjectSize();

        while (latch.getCount() > 0) {
            int currentAverageObjectSize = arrayPool.getAverageObjectSize();
            if (currentAverageObjectSize != lastAverageObjectSize) {
                lastAverageObjectSize = currentAverageObjectSize;
                LOGGER.info("Average object size in ArrayPool is: %d", currentAverageObjectSize);
            }
            Thread.sleep(RANDOM.nextInt(5));
        }

        // Check for errors
        for (int i = 0; i < threadCount; i++) {
            try {
                futures[i].get();
            }
            catch (Exception ex) {
                throw new AssertionError("Test failed in running poolClient", ex);
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private long sizeOf(Object array)
    {
        if (array instanceof byte[]) {
            return ((byte[]) array).length;
        }
        else if (array instanceof boolean[]) {
            return ((boolean[]) array).length;
        }
        else {
            return -1;
        }
    }
}
