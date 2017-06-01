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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;

public class PlanNodeCostEstimate
{
    public static final PlanNodeCostEstimate INFINITE_COST = new PlanNodeCostEstimate(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY);
    public static final PlanNodeCostEstimate UNKNOWN_COST = new PlanNodeCostEstimate(NaN, NaN, NaN);
    public static final PlanNodeCostEstimate ZERO_COST = new PlanNodeCostEstimate(0, 0, 0);

    private final double cpuCost;
    private final double memoryCost;
    private final double networkCost;

    private PlanNodeCostEstimate(double cpuCost, double memoryCost, double networkCost)
    {
        checkArgument(isNaN(cpuCost) || cpuCost >= 0, "cpuCost cannot be negative");
        checkArgument(isNaN(memoryCost) || memoryCost >= 0, "memoryCost cannot be negative");
        checkArgument(isNaN(networkCost) || networkCost >= 0, "networkCost cannot be negative");
        this.cpuCost = cpuCost;
        this.memoryCost = memoryCost;
        this.networkCost = networkCost;
    }

    /**
     * Returns CPU component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getCpuCost()
    {
        return cpuCost;
    }

    /**
     * Returns memory component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getMemoryCost()
    {
        return memoryCost;
    }

    /**
     * Returns network component of the cost. Unknown value is represented by {@link Double#NaN}
     */
    public double getNetworkCost()
    {
        return networkCost;
    }

    /**
     * Returns true if this cost has unknown components.
     */
    public boolean hasUnknownComponents()
    {
        return isNaN(cpuCost) || isNaN(memoryCost) || isNaN(networkCost);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cpuCost", cpuCost)
                .add("memoryCost", memoryCost)
                .add("networkCost", networkCost)
                .toString();
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
        return Double.compare(that.cpuCost, cpuCost) == 0 &&
                Double.compare(that.memoryCost, memoryCost) == 0 &&
                Double.compare(that.networkCost, networkCost) == 0;
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
        return builder().setCpuCost(0).setMemoryCost(0).setNetworkCost(networkCost).build();
    }

    public static PlanNodeCostEstimate cpuCost(double cpuCost)
    {
        return builder().setCpuCost(cpuCost).setMemoryCost(0).setNetworkCost(0).build();
    }

    public static PlanNodeCostEstimate memoryCost(double memoryCost)
    {
        return builder().setCpuCost(0).setMemoryCost(memoryCost).setNetworkCost(0).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Optional<Double> cpuCost = Optional.empty();
        private Optional<Double> memoryCost = Optional.empty();
        private Optional<Double> networkCost = Optional.empty();

        public Builder setCpuCost(double cpuCost)
        {
            checkState(!this.cpuCost.isPresent(), "cpuCost already set");
            this.cpuCost = Optional.of(cpuCost);
            return this;
        }

        public Builder setMemoryCost(double memoryCost)
        {
            checkState(!this.memoryCost.isPresent(), "memoryCost already set");
            this.memoryCost = Optional.of(memoryCost);
            return this;
        }

        public Builder setNetworkCost(double networkCost)
        {
            checkState(!this.networkCost.isPresent(), "networkCost already set");
            this.networkCost = Optional.of(networkCost);
            return this;
        }

        public PlanNodeCostEstimate build()
        {
            checkState(cpuCost.isPresent(), "cpuCost not set");
            checkState(memoryCost.isPresent(), "memoryCost not set");
            checkState(networkCost.isPresent(), "networkCost not set");
            return new PlanNodeCostEstimate(cpuCost.get(), memoryCost.get(), networkCost.get());
        }
    }
}
