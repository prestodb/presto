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
package com.facebook.presto;

import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TaskSource
{
    private final PlanNodeId planNodeId;
    private final Set<ScheduledSplit> splits;
    private final boolean noMoreSplits;

    @JsonCreator
    public TaskSource(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("splits") Set<ScheduledSplit> splits,
            @JsonProperty("noMoreSplits") boolean noMoreSplits)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.splits = ImmutableSet.copyOf(requireNonNull(splits, "splits is null"));
        this.noMoreSplits = noMoreSplits;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Set<ScheduledSplit> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public boolean isNoMoreSplits()
    {
        return noMoreSplits;
    }

    public TaskSource update(TaskSource source)
    {
        checkArgument(planNodeId.equals(source.getPlanNodeId()), "Expected source %s, but got source %s", planNodeId, source.getPlanNodeId());

        if (isNewer(source)) {
            // assure the new source is properly formed
            // we know that either the new source one has new splits and/or it is marking the source as closed
            checkArgument(!noMoreSplits || splits.containsAll(source.getSplits()), "Source %s has new splits, but no more splits already set", planNodeId);

            Set<ScheduledSplit> newSplits = ImmutableSet.<ScheduledSplit>builder()
                    .addAll(splits)
                    .addAll(source.getSplits())
                    .build();

            return new TaskSource(planNodeId,
                    newSplits,
                    source.isNoMoreSplits());
        }
        else {
            // the specified source is older than this one
            return this;
        }
    }

    private boolean isNewer(TaskSource source)
    {
        // the specified source is newer if it changes the no more
        // splits flag or if it contains new splits
        return (!noMoreSplits && source.isNoMoreSplits()) ||
                (!splits.containsAll(source.getSplits()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("planNodeId", planNodeId)
                .add("splits", splits)
                .add("noMoreSplits", noMoreSplits)
                .toString();
    }
}
