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

import com.facebook.presto.spi.plan.PlanNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CanonicalPlanFragment
{
    private final PlanNode plan;
    private final CanonicalPartitioningScheme partitioningScheme;

    @JsonCreator
    public CanonicalPlanFragment(
            @JsonProperty("plan") PlanNode plan,
            @JsonProperty("partitionScheme") CanonicalPartitioningScheme partitioningScheme)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
    }

    @JsonProperty
    public PlanNode getPlan()
    {
        return plan;
    }

    @JsonProperty
    public CanonicalPartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CanonicalPlanFragment that = (CanonicalPlanFragment) o;
        return Objects.equals(plan, that.plan) &&
                Objects.equals(partitioningScheme, that.partitioningScheme);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(plan, partitioningScheme);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("plan", plan)
                .add("partitioningScheme", partitioningScheme)
                .toString();
    }
}
