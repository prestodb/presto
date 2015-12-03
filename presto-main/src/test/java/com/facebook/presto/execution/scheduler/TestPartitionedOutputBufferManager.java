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
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestPartitionedOutputBufferManager
{
    private static final StageId STAGE_ID = new StageId("query", "stage");

    @Test
    public void test()
            throws Exception
    {
        AtomicReference<OutputBuffers> outputBufferTarget = new AtomicReference<>();

        PartitionedOutputBufferManager hashOutputBufferManager = new PartitionedOutputBufferManager(outputBufferTarget::set);

        // add buffers, which does not cause output buffer to be set
        assertNull(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "0"), 2);
        assertNull(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "1"), 0);
        assertNull(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "2"), 1);
        assertNull(outputBufferTarget.get());

        // set no more buffers, which causes buffers to be created
        hashOutputBufferManager.noMoreOutputBuffers();
        assertNotNull(outputBufferTarget.get());

        // verify output buffers
        OutputBuffers outputBuffers = outputBufferTarget.get();
        assertTrue(outputBuffers.getVersion() > 0);
        assertTrue(outputBuffers.isNoMoreBufferIds());

        Map<TaskId, Integer> buffers = outputBuffers.getBuffers();
        assertEquals(buffers.size(), 3);
        assertEquals(buffers.get(new TaskId(STAGE_ID, "0")), Integer.valueOf(2));
        assertEquals(buffers.get(new TaskId(STAGE_ID, "1")), Integer.valueOf(0));
        assertEquals(buffers.get(new TaskId(STAGE_ID, "2")), Integer.valueOf(1));

        // try to add another buffer, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "4"), 4);
        assertEquals(outputBuffers, outputBufferTarget.get());

        // try to set no more buffers again, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.noMoreOutputBuffers();
        assertEquals(outputBuffers, outputBufferTarget.get());
    }
}
