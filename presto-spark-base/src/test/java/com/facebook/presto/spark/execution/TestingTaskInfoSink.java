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

import com.facebook.presto.execution.TaskInfo;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.getFragmentId;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Collects the folded task infos in the order they are folded.
 */
public class TestingTaskInfoSink
        implements TaskInfoSink<List<TaskInfo>>
{
    private final List<TaskInfo> taskInfos = new ArrayList<>();
    private Thread thread;
    private boolean sealed;

    @Override
    public void addTask(int fragmentId, TaskInfo taskInfo)
    {
        checkState(!sealed, "sink is sealed");
        checkArgument(fragmentId == getFragmentId(taskInfo), "fragment id %s does not match task %s", fragmentId, taskInfo.getTaskId());
        recordThread();
        taskInfos.add(taskInfo);
    }

    @Override
    public List<TaskInfo> seal()
    {
        checkState(!sealed, "sink is already sealed");
        recordThread();
        sealed = true;
        return ImmutableList.copyOf(taskInfos);
    }

    /**
     * The thread that called the sink. Only safe to read once the aggregation is sealed.
     */
    public Thread getThread()
    {
        return thread;
    }

    private void recordThread()
    {
        if (thread == null) {
            thread = Thread.currentThread();
        }
        checkState(thread == Thread.currentThread(), "sink called from %s and %s", thread, Thread.currentThread());
    }
}
