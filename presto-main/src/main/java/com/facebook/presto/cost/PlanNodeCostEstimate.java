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
package com.facebook.presto.cost;

import java.util.Objects;

import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;

public class PlanNodeCostEstimate
{
    public static final PlanNodeCostEstimate INFINITE_COST = new PlanNodeCostEstimate(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);
    public static final PlanNodeCostEstimate UNKNOWN_COST = new PlanNodeCostEstimate(NaN, NaN, NaN);
    public static final PlanNodeCostEstimate ZERO_COST = PlanNodeCostEstimate.builder().build();

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    private PlanNodeCostEstimate(double cpuCost, double memoryCost, double networkCost)
    {
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    public double getCpuCost()
    {
        return cpuCost;
    }

    public double getMemoryCost()
    {
        return memoryCost;
    }

    public double getNetworkCost()
    {
        return networkCost;
    }

    public boolean isUnknown()
    {
        return isNaN(cpuCost) || isNaN(memoryCost) || isNaN(networkCost);
    }

    @Override
    public String toString()
    {
        return "PlanNodeCostEstimate{" +
                "cpuCost=" + cpuCost +
                ", memoryCost=" + memoryCost +
                ", networkCost=" + networkCost + '}';
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
        PlanNodeCostEstimate that = (PlanNodeCostEstimate) o;
        return Objects.equals(cpuCost, that.cpuCost) &&
                Objects.equals(memoryCost, that.memoryCost) &&
                Objects.equals(networkCost, that.networkCost);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuCost, memoryCost, networkCost);
    }

    public PlanNodeCostEstimate add(PlanNodeCostEstimate other)
    {
        return new PlanNodeCostEstimate(
                cpuCost + other.cpuCost,
                memoryCost + other.memoryCost,
                networkCost + other.networkCost);
    }

    public static PlanNodeCostEstimate networkCost(double networkCost)
    {
        return builder().setNetworkCost(networkCost).build();
    }

    public static PlanNodeCostEstimate cpuCost(double cpuCost)
    {
        return builder().setCpuCost(cpuCost).build();
    }

    public static PlanNodeCostEstimate memoryCost(double memoryCost)
    {
        return builder().setCpuCost(memoryCost).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private double cpuCost = 0;
        private double memoryCost = 0;
        private double networkCost = 0;

        public Builder setFrom(PlanNodeCostEstimate otherStatistics)
        {
            return setCpuCost(otherStatistics.getCpuCost())
                    .setMemoryCost(otherStatistics.getMemoryCost())
                    .setNetworkCost(otherStatistics.getNetworkCost());
        }

        public Builder setCpuCost(double cpuCost)
        {
            this.cpuCost = cpuCost;
            return this;
        }

        public Builder setMemoryCost(double memoryCost)
        {
            this.memoryCost = memoryCost;
            return this;
        }

        public Builder setNetworkCost(double networkCost)
        {
            this.networkCost = networkCost;
            return this;
        }

        public PlanNodeCostEstimate build()
        {
            return new PlanNodeCostEstimate(cpuCost, memoryCost, networkCost);
        }
    }
}
