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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class CanonicalPlanWithInfo
{
    private final CanonicalPlan canonicalPlan;
    private final PlanNodeCanonicalInfo info;

    @JsonCreator
    public CanonicalPlanWithInfo(@JsonProperty("canonicalPlan") CanonicalPlan canonicalPlan, @JsonProperty("info") PlanNodeCanonicalInfo info)
    {
        this.canonicalPlan = requireNonNull(canonicalPlan, "canonicalPlan is null");
        this.info = requireNonNull(info, "info is null");
    }

    @JsonProperty("canonicalPlan")
    public CanonicalPlan getCanonicalPlan()
    {
        return canonicalPlan;
    }

    @JsonProperty("info")
    public PlanNodeCanonicalInfo getInfo()
    {
        return info;
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
        CanonicalPlanWithInfo that = (CanonicalPlanWithInfo) o;
        return Objects.equals(canonicalPlan, that.canonicalPlan) && Objects.equals(info, that.info);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(canonicalPlan, info);
    }
}
