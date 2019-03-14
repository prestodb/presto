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
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.graph.SuccessorsFunction;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.util.Objects.requireNonNull;

public class StageExecutionPlan
{
    private final PlanFragment fragment;
    private final Map<PlanNodeId, SplitSource> splitSources;
    private final List<StageExecutionPlan> subStages;
    private final Optional<List<String>> fieldNames;

    public StageExecutionPlan(
            PlanFragment fragment,
            Map<PlanNodeId, SplitSource> splitSources,
            List<StageExecutionPlan> subStages)
    {
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.splitSources = requireNonNull(splitSources, "dataSource is null");
        this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "dependencies is null"));

        fieldNames = (fragment.getRoot() instanceof OutputNode) ?
                Optional.of(ImmutableList.copyOf(((OutputNode) fragment.getRoot()).getColumnNames())) :
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

    public Map<PlanNodeId, SplitSource> getSplitSources()
    {
        return splitSources;
    }

    public List<StageExecutionPlan> getSubStages()
    {
        return subStages;
    }

    public StageExecutionPlan withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new StageExecutionPlan(fragment.withBucketToPartition(bucketToPartition), splitSources, subStages);
    }

    public Stream<StageExecutionPlan> geAllStages()
    {
        return stream(forTree(StageExecutionPlan::getSubStages).depthFirstPreOrder(this));
    }

    public Stream<StageExecutionPlan> geAllRemoteSourceLinkedStages()
    {
        SuccessorsFunction<StageExecutionPlan> remoteSourceLinked = node -> {
            Set<PlanFragmentId> remoteSources = node.getFragment().getRemoteSourceNodes().stream()
                    .map(RemoteSourceNode::getSourceFragmentIds)
                    .flatMap(List::stream)
                    .collect(toImmutableSet());
            return node.getSubStages().stream()
                    .filter(subStage -> remoteSources.contains(subStage.getFragment().getId()))
                    .collect(toImmutableList());
        };
        return stream(forTree(remoteSourceLinked).depthFirstPreOrder(this));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fragment", fragment)
                .add("splitSources", splitSources)
                .add("subStages", subStages)
                .toString();
    }
}
