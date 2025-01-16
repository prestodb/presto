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
package com.facebook.presto.execution;

import com.facebook.presto.spi.QueryId;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestId
{
    @Test
    public void testAppendString()
    {
        QueryId queryId = new QueryId("foo");
        StageId stageId = new StageId(queryId, 1);
        StageExecutionId stageExecutionId = new StageExecutionId(stageId, 2);
        TaskId taskId = new TaskId(stageExecutionId, 3, 1);

        assertEquals(queryId.toString(), "foo");
        assertEquals(stageId.toString(), String.format("%s.%s", queryId.getId(), stageId.getId()));
        assertEquals(stageExecutionId.toString(), String.format("%s.%s.%s", queryId.getId(), stageId.getId(), stageExecutionId.getId()));
        assertEquals(taskId.toString(), String.format("%s.%s.%s.%s.%s", queryId.getId(), stageId.getId(), stageExecutionId.getId(), taskId.getId(), taskId.getAttemptNumber()));
    }
}
