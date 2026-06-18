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
package com.facebook.presto.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Integer.max;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestingCacheUtils
{
    private TestingCacheUtils()
    {
    }

    public static void validateBuffer(byte[] data, long position, byte[] buffer, int offset, int length)
    {
        for (int i = 0; i < length; i++) {
            assertEquals(buffer[i + offset], data[i + (int) position], format("corrupted buffer at position %s offset %s", position, i));
        }
    }

    public static void stressTest(byte[] data, TestingReadOperation testingReadOperation)
            throws ExecutionException, InterruptedException
    {
        ExecutorService executor = newScheduledThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();
        AtomicReference<String> exception = new AtomicReference<>();

        for (int i = 0; i < 5; i++) {
            byte[] buffer = new byte[data.length];
            futures.add(executor.submit(() -> {
                Random random = new Random();
                for (int j = 0; j < 200; j++) {
                    int position = random.nextInt(data.length - 1);
                    int length = random.nextInt(max((data.length - position) / 3, 1));
                    int offset = random.nextInt(data.length - length);

                    try {
                        testingReadOperation.invoke(position, buffer, offset, length);
                    }
                    catch (IOException e) {
                        exception.compareAndSet(null, e.getMessage());
                        return;
                    }
                    validateBuffer(data, position, buffer, offset, length);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        if (exception.get() != null) {
            fail(exception.get());
        }
    }

    public interface TestingReadOperation
    {
        void invoke(long position, byte[] buffer, int offset, int length) throws IOException;
    }
}
