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

import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

import static com.facebook.presto.spi.StandardErrorCode.PLAN_SERIALIZATION_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CanonicalPlan
{
    private final PlanNode plan;
    private final PlanCanonicalizationStrategy strategy;

    @JsonCreator
    public CanonicalPlan(
            @JsonProperty("plan") PlanNode plan,
            @JsonProperty("strategy") PlanCanonicalizationStrategy strategy)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.strategy = requireNonNull(strategy, "plan is null");
    }

    @JsonProperty
    public PlanNode getPlan()
    {
        return plan;
    }

    @JsonProperty
    public PlanCanonicalizationStrategy getStrategy()
    {
        return strategy;
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
        CanonicalPlan that = (CanonicalPlan) o;
        return Objects.equals(plan, that.plan) &&
                Objects.equals(strategy, that.strategy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(plan, strategy);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("plan", plan)
                .add("strategy", strategy)
                .toString();
    }

    public String toString(ObjectMapper objectMapper)
    {
        try {
            return objectMapper.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(PLAN_SERIALIZATION_ERROR, "Cannot serialize plan to JSON", e);
        }
    }
}
