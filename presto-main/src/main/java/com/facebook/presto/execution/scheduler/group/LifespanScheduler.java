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
package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.SourceScheduler;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;

public interface LifespanScheduler
{
    // Thread Safety:
    // * Invocation of onLifespanExecutionFinished can be parallel and in any thread.
    //   There may be multiple invocations in flight at the same time,
    //   and may overlap with any other methods.
    // * Invocation of schedule happens sequentially in a single thread.
    // * This object is safely published after invoking scheduleInitial.

    void scheduleInitial(SourceScheduler scheduler);

    void onLifespanExecutionFinished(Iterable<Lifespan> newlyCompletelyExecutedDriverGroups);

    void onTaskFailed(int taskId, List<SourceScheduler> sourceSchedulers);

    SettableFuture schedule(SourceScheduler scheduler);

    boolean allLifespanExecutionFinished();
}
