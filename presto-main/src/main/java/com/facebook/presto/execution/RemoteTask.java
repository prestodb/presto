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

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.units.Duration;

import java.util.Set;

public interface RemoteTask
{
    String getNodeId();

    TaskInfo getTaskInfo();

    void start();

    void addSplit(PlanNodeId sourceId, Split split);

    void noMoreSplits(PlanNodeId sourceId);

    void addOutputBuffers(Set<String> outputBuffers, boolean noMore);

    void addStateChangeListener(StateChangeListener<TaskInfo> stateChangeListener);

    void cancel();

    int getQueuedSplits();

    Duration waitForTaskToFinish(Duration maxWait)
            throws InterruptedException;
}
