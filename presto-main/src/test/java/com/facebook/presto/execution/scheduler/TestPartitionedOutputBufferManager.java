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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPartitionedOutputBufferManager
{
    @Test
    public void test()
            throws Exception
    {
        AtomicReference<OutputBuffers> outputBufferTarget = new AtomicReference<>();

        PartitionedOutputBufferManager hashOutputBufferManager = new PartitionedOutputBufferManager(4, outputBufferTarget::set);

        // output buffers are set immediately when the manager is created
        assertOutputBuffers(outputBufferTarget.get());

        // add buffers, which does not cause an error
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(0)), false);
        assertOutputBuffers(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(3)), true);
        assertOutputBuffers(outputBufferTarget.get());

        // try to a buffer out side of the partition range, which should result in an error
        try {
            hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(5)), false);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException e) {
        }
        assertOutputBuffers(outputBufferTarget.get());

        // try to a buffer out side of the partition range, which should result in an error
        try {
            hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(6)), true);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException e) {
        }
        assertOutputBuffers(outputBufferTarget.get());
    }

    private static void assertOutputBuffers(OutputBuffers outputBuffers)
    {
        assertNotNull(outputBuffers);
        assertTrue(outputBuffers.getVersion() > 0);
        assertTrue(outputBuffers.isNoMoreBufferIds());
        Map<OutputBufferId, Integer> buffers = outputBuffers.getBuffers();
        assertEquals(buffers.size(), 4);
        for (int partition = 0; partition < 4; partition++) {
            assertEquals(buffers.get(new OutputBufferId(partition)), Integer.valueOf(partition));
        }
    }
}
