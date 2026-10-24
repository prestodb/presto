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

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTaskSource
{
    private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("plan-node");

    @Test
    public void testSplitsAreOrderedBySequenceId()
    {
        TaskSource taskSource = new TaskSource(
                PLAN_NODE_ID,
                ImmutableSet.of(createScheduledSplit(16), createScheduledSplit(1)),
                true);

        assertThat(taskSource.getSplits())
                .extracting(ScheduledSplit::getSequenceId)
                .containsExactly(1L, 16L);
    }

    private static ScheduledSplit createScheduledSplit(long sequenceId)
    {
        return new ScheduledSplit(
                sequenceId,
                PLAN_NODE_ID,
                new Split(new ConnectorId("test"), new TestingTransactionHandle(new UUID(0, 0)), TestingSplit.createRemoteSplit()));
    }
}
