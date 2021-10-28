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
package com.facebook.presto.tracing.testing;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.tracing.SimpleTracer;
import com.facebook.presto.tracing.SimpleTracerProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.spi.StandardErrorCode.DISTRIBUTED_TRACING_ERROR;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestSimpleTracer
{
    private final SimpleTracerProvider tracerProvider = new SimpleTracerProvider();
    private final ExecutorService executor = newFixedThreadPool(16, daemonThreadsNamed("presto-testing-tracer-threadpool"));
    private final Random random = new Random();
    private final int numThreads = 10;

    public TestSimpleTracer()
    {
    }

    @Test
    public void testAddPoint()
    {
        SimpleTracer tracer = (SimpleTracer) tracerProvider.getNewTracer();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            CompletableFuture<?> future = new CompletableFuture<>();
            executor.submit(() -> {
                for (int j = 0; j < 20; j++) {
                    tracer.addPoint("test-point");
                    try {
                        Thread.sleep(random.nextInt(5));
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                future.complete(null);
            });
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[numThreads])).thenApply(v -> {
            tracer.endTrace("trace ended");
            assertEquals(tracer.pointList.size(), 202);
            return null;
        }).join();
    }

    @Test
    public void testAddBlock()
    {
        SimpleTracer tracer = (SimpleTracer) tracerProvider.getNewTracer();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            CompletableFuture<?> future = new CompletableFuture<>();
            int threadNum = i;
            executor.submit(() -> {
                for (int j = 0; j < 20; j++) {
                    tracer.startBlock("test-block-" + threadNum + "." + j, "");
                    tracer.addPointToBlock("test-block-" + threadNum + "." + j, "point added");
                    try {
                        Thread.sleep(random.nextInt(20));
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    tracer.endBlock("test-block-" + threadNum + "." + j, "");
                }
                future.complete(null);
            });
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[numThreads])).thenApply(v -> {
            tracer.endTrace("trace ended");
            assertEquals(tracer.recorderBlockMap.size(), 200);
            return null;
        }).join();
    }

    @Test
    public void testBlockErrors()
    {
        SimpleTracer tracer = (SimpleTracer) tracerProvider.getNewTracer();

        // Duplicate block
        PrestoException exception = expectThrows(PrestoException.class, () -> {
            tracer.startBlock("test-block", "");
            tracer.startBlock("test-block", "");
        });
        assertEquals(exception.getErrorCode(), DISTRIBUTED_TRACING_ERROR.toErrorCode());
        tracer.endBlock("test-block", "");

        // Deleting non-existing block
        exception = expectThrows(PrestoException.class, () -> {
            tracer.startBlock("test-block", "");
            tracer.endBlock("test-block-non-existing", "");
        });
        assertEquals(exception.getErrorCode(), DISTRIBUTED_TRACING_ERROR.toErrorCode());
        tracer.endBlock("test-block", "");

        // Adding point to non-existing block
        exception = expectThrows(PrestoException.class, () -> tracer.addPointToBlock("test-block", "Adding point to non-existing block"));
        assertEquals(exception.getErrorCode(), DISTRIBUTED_TRACING_ERROR.toErrorCode());
    }
}
