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

import com.facebook.presto.HashPagePartitionFunction;
import com.facebook.presto.OutputBuffers;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.PlanFragment.NullPartitioning.HASH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestHashPartitionFunctionGenerator
{
    private static final StageId STAGE_ID = new StageId("query", "stage");

    private static final ImmutableList<Integer> PARTITION_CHANNELS = ImmutableList.of(0);
    private static final Optional<Integer> HASH_CHANNEL = Optional.of(1);
    private static final ImmutableList<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR);
    private static final HashPartitionFunctionGenerator PARTITION_FUNCTION_GENERATOR = new HashPartitionFunctionGenerator(PARTITION_CHANNELS, HASH_CHANNEL, TYPES, HASH);

    @Test
    public void test()
            throws Exception
    {
        AtomicReference<OutputBuffers> outputBufferTarget = new AtomicReference<>();
        PartitionedOutputBufferManager hashOutputBufferManager = new PartitionedOutputBufferManager(outputBufferTarget::set, PARTITION_FUNCTION_GENERATOR);

        // add buffers, which does not cause output buffer to be set
        assertNull(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "0"));
        assertNull(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "1"));
        assertNull(outputBufferTarget.get());
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "2"));
        assertNull(outputBufferTarget.get());

        // set no more buffers, which causes buffers to be created
        hashOutputBufferManager.noMoreOutputBuffers();
        assertNotNull(outputBufferTarget.get());

        // verify output buffers
        OutputBuffers outputBuffers = outputBufferTarget.get();
        assertTrue(outputBuffers.getVersion() > 0);
        assertTrue(outputBuffers.isNoMoreBufferIds());

        Map<TaskId, PagePartitionFunction> buffers = outputBuffers.getBuffers();
        assertEquals(buffers.size(), 3);
        assertEquals(buffers.get(new TaskId(STAGE_ID, "0")), new HashPagePartitionFunction(0, 3, PARTITION_CHANNELS, HASH_CHANNEL, TYPES, HASH));
        assertEquals(buffers.get(new TaskId(STAGE_ID, "1")), new HashPagePartitionFunction(1, 3, PARTITION_CHANNELS, HASH_CHANNEL, TYPES, HASH));
        assertEquals(buffers.get(new TaskId(STAGE_ID, "2")), new HashPagePartitionFunction(2, 3, PARTITION_CHANNELS, HASH_CHANNEL, TYPES, HASH));

        // try to add another buffer, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.addOutputBuffer(new TaskId(STAGE_ID, "4"));
        assertEquals(outputBuffers, outputBufferTarget.get());

        // try to set no more buffers again, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.noMoreOutputBuffers();
        assertEquals(outputBuffers, outputBufferTarget.get());
    }
}
