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
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Folds the task infos of each fragment into {@link FragmentTaskAggregates}.
 */
public class FragmentTaskAggregatesSink
        implements TaskInfoSink<Map<PlanFragmentId, FragmentTaskAggregates>>
{
    private final Map<PlanFragmentId, FragmentTaskAggregates> fragments = new HashMap<>();

    @Override
    public void addTask(int fragmentId, TaskInfo taskInfo)
    {
        fragments.computeIfAbsent(new PlanFragmentId(fragmentId), ignored -> new FragmentTaskAggregates()).addTask(taskInfo);
    }

    @Override
    public Map<PlanFragmentId, FragmentTaskAggregates> seal()
    {
        return ImmutableMap.copyOf(fragments);
    }
}
