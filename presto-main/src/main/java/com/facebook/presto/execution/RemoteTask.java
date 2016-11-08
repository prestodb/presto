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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;

import java.util.concurrent.CompletableFuture;

public interface RemoteTask
{
    TaskId getTaskId();

    String getNodeId();

    TaskInfo getTaskInfo();

    TaskStatus getTaskStatus();

    void start();

    void addSplits(Multimap<PlanNodeId, Split> splits);

    void noMoreSplits(PlanNodeId sourceId);

    void setOutputBuffers(OutputBuffers outputBuffers);

    void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener);

    CompletableFuture<?> whenSplitQueueHasSpace(int threshold);

    void cancel();

    void abort();

    int getPartitionedSplitCount();

    int getQueuedPartitionedSplitCount();
}
