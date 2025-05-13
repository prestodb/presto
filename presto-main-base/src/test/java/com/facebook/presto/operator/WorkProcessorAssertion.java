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

import com.google.common.util.concurrent.SettableFuture;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class WorkProcessorAssertion
{
    private WorkProcessorAssertion() {}

    public static <T> void assertBlocks(WorkProcessor<T> processor)
    {
        assertFalse(processor.process());
        assertTrue(processor.isBlocked());
        assertFalse(processor.isFinished());
        assertFalse(processor.process());
    }

    public static <T, V> void assertUnblocks(WorkProcessor<T> processor, SettableFuture<V> future)
    {
        future.set(null);
        assertFalse(processor.isBlocked());
    }

    public static <T> void assertYields(WorkProcessor<T> processor)
    {
        assertFalse(processor.process());
        assertFalse(processor.isBlocked());
        assertFalse(processor.isFinished());
    }

    public static <T> void assertResult(WorkProcessor<T> processor, T result)
    {
        assertTrue(processor.process());
        assertFalse(processor.isBlocked());
        assertFalse(processor.isFinished());
        assertEquals(processor.getResult(), result);
    }

    public static <T> void assertFinishes(WorkProcessor<T> processor)
    {
        assertTrue(processor.process());
        assertFalse(processor.isBlocked());
        assertTrue(processor.isFinished());
        assertTrue(processor.process());
    }
}
