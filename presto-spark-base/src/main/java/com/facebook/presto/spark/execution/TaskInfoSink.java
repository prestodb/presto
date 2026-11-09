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

/**
 * Consumes the task infos selected by {@link TaskInfoAggregator}. All calls are made from the aggregator thread, so
 * implementations do not need to be thread safe.
 *
 * @param <R> the aggregated result, typically keyed by fragment id
 */
public interface TaskInfoSink<R>
{
    /**
     * Called at most once per (fragment, partition) with the selected attempt of that task. The task info must not be
     * retained.
     */
    void addTask(int fragmentId, TaskInfo taskInfo);

    /**
     * Called once, after the last {@link #addTask}.
     */
    R seal();
}
