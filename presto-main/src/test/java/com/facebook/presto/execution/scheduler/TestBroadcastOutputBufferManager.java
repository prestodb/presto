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

import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static org.testng.Assert.assertEquals;

public class TestBroadcastOutputBufferManager
{
    @Test
    public void test()
            throws Exception
    {
        AtomicReference<OutputBuffers> outputBufferTarget = new AtomicReference<>();
        BroadcastOutputBufferManager hashOutputBufferManager = new BroadcastOutputBufferManager(outputBufferTarget::set);
        assertEquals(outputBufferTarget.get(), createInitialEmptyOutputBuffers(BROADCAST));

        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(0)), false);
        OutputBuffers expectedOutputBuffers = createInitialEmptyOutputBuffers(BROADCAST).withBuffer(new OutputBufferId(0), BROADCAST_PARTITION_ID);
        assertEquals(outputBufferTarget.get(), expectedOutputBuffers);

        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(1), new OutputBufferId(2)), false);

        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new OutputBufferId(1), BROADCAST_PARTITION_ID);
        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new OutputBufferId(2), BROADCAST_PARTITION_ID);
        assertEquals(outputBufferTarget.get(), expectedOutputBuffers);

        // set no more buffers
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(3)), true);
        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new OutputBufferId(3), BROADCAST_PARTITION_ID);
        expectedOutputBuffers = expectedOutputBuffers.withNoMoreBufferIds();
        assertEquals(outputBufferTarget.get(), expectedOutputBuffers);

        // try to add another buffer, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(5)), false);
        assertEquals(outputBufferTarget.get(), expectedOutputBuffers);

        // try to set no more buffers again, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.addOutputBuffers(ImmutableList.of(new OutputBufferId(6)), true);
        assertEquals(outputBufferTarget.get(), expectedOutputBuffers);
    }
}
