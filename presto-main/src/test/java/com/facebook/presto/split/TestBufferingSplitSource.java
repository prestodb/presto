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
package com.facebook.presto.split;

import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBufferingSplitSource
{
    @Test
    public void testSlowSource()
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource(1, 25);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            assertEquals(source.getNextBatch(20).get().size(), 10);
            assertEquals(source.getNextBatch(6).get().size(), 6);
            assertEquals(source.getNextBatch(20).get().size(), 9);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchCalls(), 25);
        }
    }

    @Test
    public void testFastSource()
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource(11, 22);
        try (SplitSource source = new BufferingSplitSource(mockSource, 10)) {
            assertEquals(source.getNextBatch(200).get().size(), 11);
            assertEquals(source.getNextBatch(200).get().size(), 11);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchCalls(), 2);
        }
    }

    @Test
    public void testEmptySource()
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource(1, 0);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            assertEquals(source.getNextBatch(200).get().size(), 0);
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchCalls(), 0);
        }
    }

    @Test
    public void testFailImmediate()
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource(1, 1, 0);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            try {
                source.getNextBatch(200).get();
                fail();
            }
            catch (IllegalStateException e) {
                assertEquals(e.getMessage(), "Mock failure");
            }
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchCalls(), 1);
        }
    }

    @Test
    public void testFail()
            throws Exception
    {
        MockSplitSource mockSource = new MockSplitSource(1, 2, 1);
        try (SplitSource source = new BufferingSplitSource(mockSource, 100)) {
            try {
                source.getNextBatch(2).get();
                fail();
            }
            catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof IllegalStateException);
                assertEquals(e.getCause().getMessage(), "Mock failure");
            }
            assertTrue(source.isFinished());
            assertEquals(mockSource.getNextBatchCalls(), 2);
        }
    }
}
