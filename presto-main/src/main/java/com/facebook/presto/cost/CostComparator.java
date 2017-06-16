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

import com.facebook.presto.Session;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;

import java.util.Comparator;

import static com.google.common.base.Preconditions.checkArgument;

public class CostComparator
{
    private final double cpuWeight;
    private final double memoryWeight;
    private final double networkWeight;

    @Inject
    public CostComparator(FeaturesConfig featuresConfig)
    {
        this(featuresConfig.getCpuCostWeight(), featuresConfig.getMemoryCostWeight(), featuresConfig.getNetworkCostWeight());
    }

    @VisibleForTesting
    public CostComparator(double cpuWeight, double memoryWeight, double networkWeight)
    {
        this.cpuWeight = cpuWeight;
        this.memoryWeight = memoryWeight;
        this.networkWeight = networkWeight;
    }

    public Comparator<PlanNodeCostEstimate> forSession(Session session)
    {
        return (left, right) -> CostComparator.this.compare(session, left, right);
    }

    public int compare(Session session, PlanNodeCostEstimate left, PlanNodeCostEstimate right)
    {
        checkArgument(!left.isUnknown() && !right.isUnknown(), "cannot compare unknown costs");
        double leftCost = left.getCpuCost() * cpuWeight
                + left.getMemoryCost() * memoryWeight
                + left.getNetworkCost() * networkWeight;

        double rightCost = right.getCpuCost() * cpuWeight
                + right.getMemoryCost() * memoryWeight
                + right.getNetworkCost() * networkWeight;

        return Double.compare(leftCost, rightCost);
    }
}
