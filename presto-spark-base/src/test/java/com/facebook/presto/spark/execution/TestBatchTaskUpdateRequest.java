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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static org.testng.Assert.assertEquals;

public class TestBatchTaskUpdateRequest
{
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final JsonCodec<BatchTaskUpdateRequest> BATCH_TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(BatchTaskUpdateRequest.class);

    @Test
    public void testJsonConversion()
    {
        List<TaskSource> sources = new ArrayList<>();
        Session session = TestingSession.testSessionBuilder().build();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                Optional.of(createPlanFragment().toBytes(PLAN_FRAGMENT_JSON_CODEC)),
                sources,
                createInitialEmptyOutputBuffers(PARTITIONED),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty())));
        Map<String, byte[]> shuffleReadInfos = new HashMap<>();
        shuffleReadInfos.put("dummy-plan-node-id-0", "dummy-shuffle-read-info".getBytes());
        String shuffleWriteInfo = "dummy-shuffle-write-info";
        BatchTaskUpdateRequest batchUpdateRequest = new BatchTaskUpdateRequest(updateRequest, Optional.of(shuffleReadInfos), Optional.of(shuffleWriteInfo.getBytes()));
        byte[] batchUpdateRequestJson = BATCH_TASK_UPDATE_REQUEST_JSON_CODEC.toBytes(batchUpdateRequest);
        BatchTaskUpdateRequest recoveredBatchUpdateRequest = BATCH_TASK_UPDATE_REQUEST_JSON_CODEC.fromBytes(batchUpdateRequestJson);

        assertEquals(
                recoveredBatchUpdateRequest
                        .getShuffleReadInfos()
                        .get()
                        .get("dummy-plan-node-id-0"),
                batchUpdateRequest
                        .getShuffleReadInfos()
                        .get()
                        .get("dummy-plan-node-id-0"));
        assertEquals(batchUpdateRequest.getShuffleWriteInfo().get(), recoveredBatchUpdateRequest.getShuffleWriteInfo().get());
        assertEquals(batchUpdateRequest.getTaskUpdateRequest().getExtraCredentials(), recoveredBatchUpdateRequest.getTaskUpdateRequest().getExtraCredentials());
        assertEquals(batchUpdateRequest.getTaskUpdateRequest().getSession().getCatalog(), recoveredBatchUpdateRequest.getTaskUpdateRequest().getSession().getCatalog());
    }
}
