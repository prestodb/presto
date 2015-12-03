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
package com.facebook.presto.sql.planner;

import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class StageExecutionPlan
{
    private final PlanFragment fragment;
    private final Optional<SplitSource> dataSource;
    private final List<StageExecutionPlan> subStages;
    private final Optional<List<String>> fieldNames;

    public StageExecutionPlan(
            PlanFragment fragment,
            Optional<SplitSource> dataSource,
            List<StageExecutionPlan> subStages)
    {
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "dependencies is null"));

        fieldNames = (fragment.getRoot() instanceof OutputNode) ?
                Optional.<List<String>>of(ImmutableList.copyOf(((OutputNode) fragment.getRoot()).getColumnNames())) :
                Optional.empty();
    }

    public List<String> getFieldNames()
    {
        checkState(fieldNames.isPresent(), "cannot get field names from non-output stage");
        return fieldNames.get();
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public Optional<SplitSource> getDataSource()
    {
        return dataSource;
    }

    public List<StageExecutionPlan> getSubStages()
    {
        return subStages;
    }

    public StageExecutionPlan withPartitionCount(OptionalInt partitionCount)
    {
        return new StageExecutionPlan(fragment.withPartitionCount(partitionCount), dataSource, subStages);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fragment", fragment)
                .add("dataSource", dataSource)
                .add("subStages", subStages)
                .toString();
    }
}
