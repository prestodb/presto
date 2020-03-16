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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * StreamingSubPlan is similar to SubPlan but only contains streaming children
 */
public class StreamingSubPlan
{
    private final PlanFragment fragment;
    // streaming children
    private final List<StreamingSubPlan> children;

    public StreamingSubPlan(PlanFragment fragment, List<StreamingSubPlan> children)
    {
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<StreamingSubPlan> getChildren()
    {
        return children;
    }

    public StreamingSubPlan withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new StreamingSubPlan(fragment.withBucketToPartition(bucketToPartition), children);
    }
}
