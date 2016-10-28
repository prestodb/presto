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

import com.facebook.presto.spi.statistics.Estimate;

import java.util.function.Function;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Objects.requireNonNull;

public class PlanNodeCost
{
    public static final PlanNodeCost UNKNOWN_COST = PlanNodeCost.builder().build();

    private final Estimate outputRowCount;
    private final Estimate outputSizeInBytes;

    private PlanNodeCost(Estimate outputRowCount, Estimate outputSizeInBytes)
    {
        this.outputRowCount = requireNonNull(outputRowCount, "outputRowCount can not be null");
        this.outputSizeInBytes = requireNonNull(outputSizeInBytes, "outputSizeInBytes can not be null");
    }

    public Estimate getOutputRowCount()
    {
        return outputRowCount;
    }

    public Estimate getOutputSizeInBytes()
    {
        return outputSizeInBytes;
    }

    public PlanNodeCost mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return builder().setFrom(this).setOutputRowCount(outputRowCount.map(mappingFunction)).build();
    }

    public PlanNodeCost mapOutputSizeInBytes(Function<Double, Double> mappingFunction)
    {
        return builder().setFrom(this).setOutputSizeInBytes(outputRowCount.map(mappingFunction)).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate outputRowCount = unknownValue();
        private Estimate outputSizeInBytes = unknownValue();

        public Builder setFrom(PlanNodeCost otherStatistics)
        {
            return setOutputRowCount(otherStatistics.getOutputRowCount())
                    .setOutputSizeInBytes(otherStatistics.getOutputSizeInBytes());
        }

        public Builder setOutputRowCount(Estimate outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setOutputSizeInBytes(Estimate outputSizeInBytes)
        {
            this.outputSizeInBytes = outputSizeInBytes;
            return this;
        }

        public PlanNodeCost build()
        {
            return new PlanNodeCost(outputRowCount, outputSizeInBytes);
        }
    }
}
