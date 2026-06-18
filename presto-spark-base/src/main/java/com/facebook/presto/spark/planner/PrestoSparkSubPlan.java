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
package com.facebook.presto.spark.planner;

import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PrestoSparkSubPlan
{
    private final PlanFragment fragment;
    // single TaskSource per table scan
    private final List<TaskSource> taskSources;
    private final List<PrestoSparkSubPlan> children;

    public PrestoSparkSubPlan(PlanFragment fragment, List<TaskSource> taskSources, List<PrestoSparkSubPlan> children)
    {
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.taskSources = ImmutableList.copyOf(requireNonNull(taskSources, "taskSources is null"));
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<TaskSource> getTaskSources()
    {
        return taskSources;
    }

    public List<PrestoSparkSubPlan> getChildren()
    {
        return children;
    }
}
