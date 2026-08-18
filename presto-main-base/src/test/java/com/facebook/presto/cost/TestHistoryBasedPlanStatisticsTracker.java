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
package com.facebook.presto.cost;

import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.cost.HistoryBasedPlanStatisticsTracker.getFinishedStages;
import static com.facebook.presto.execution.StageExecutionState.ABORTED;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHistoryBasedPlanStatisticsTracker
{
    private static final QueryId QUERY_ID = new QueryId("test_query");

    @Test
    public void testAllStagesFinished()
    {
        assertStageIds(getFinishedStages(stages(FINISHED, FINISHED, FINISHED)), 0, 1, 2);
    }

    @Test
    public void testCanceledStagesAreNotTracked()
    {
        // A stage can be canceled while the query itself succeeds, for example when a limit above it is satisfied
        assertStageIds(getFinishedStages(stages(FINISHED, CANCELED)), 0);
        assertStageIds(getFinishedStages(stages(FINISHED, FINISHED, CANCELED)), 0, 1);
        assertTrue(getFinishedStages(stages(CANCELED, CANCELED)).isEmpty());
    }

    @Test
    public void testFailedAndAbortedStagesAreNotTracked()
    {
        assertStageIds(getFinishedStages(stages(ABORTED, FINISHED)), 1);
        assertStageIds(getFinishedStages(stages(FAILED, FINISHED)), 1);
        assertTrue(getFinishedStages(stages(FAILED, ABORTED)).isEmpty());
    }

    private static void assertStageIds(List<StageInfo> stages, int... expectedStageIds)
    {
        assertEquals(
                stages.stream().map(stage -> stage.getStageId().getId()).collect(toImmutableList()),
                Arrays.stream(expectedStageIds).boxed().collect(toImmutableList()));
    }

    /**
     * Builds a chain of stages where the first state given belongs to the output stage, and every following stage is a
     * source of the one before it.
     */
    private static StageInfo stages(StageExecutionState... states)
    {
        StageInfo stage = null;
        for (int stageId = states.length - 1; stageId >= 0; stageId--) {
            stage = new StageInfo(
                    new StageId(QUERY_ID, stageId),
                    URI.create("http://127.0.0.1"),
                    Optional.empty(),
                    new StageExecutionInfo(states[stageId], StageExecutionStats.zero(stageId), ImmutableList.of(), Optional.empty()),
                    ImmutableList.of(),
                    stage == null ? ImmutableList.of() : ImmutableList.of(stage),
                    false);
        }
        return stage;
    }
}
