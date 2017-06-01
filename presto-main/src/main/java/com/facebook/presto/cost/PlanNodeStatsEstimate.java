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
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class PlanNodeStatsEstimate
{
    public static final PlanNodeStatsEstimate UNKNOWN_STATS = builder().build();
    public static final double DEFAULT_ROW_SIZE = 42;

    private final double outputRowCount;
    private final double outputSizeInBytes;

    private PlanNodeStatsEstimate(double outputRowCount, double outputSizeInBytes)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative");
        checkArgument(isNaN(outputSizeInBytes) || outputSizeInBytes >= 0, "outputSizeInBytes cannot be negative");
        this.outputRowCount = outputRowCount;
        this.outputSizeInBytes = outputSizeInBytes;
    }

    /**
     * Returns estimated number of rows.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    /**
     * Returns estimated data size.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeInBytes()
    {
        return outputSizeInBytes;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(mappingFunction.apply(outputRowCount)).build();
    }

    public PlanNodeStatsEstimate mapOutputSizeInBytes(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputSizeInBytes(mappingFunction.apply(outputRowCount)).build();
    }

    @Override
    public String toString()
    {
        return "PlanNodeStatsEstimate{outputRowCount=" + outputRowCount + ", outputSizeInBytes=" + outputSizeInBytes + '}';
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
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Objects.equals(outputRowCount, that.outputRowCount) &&
                Objects.equals(outputSizeInBytes, that.outputSizeInBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, outputSizeInBytes);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return builder().setOutputRowCount(other.getOutputRowCount())
                .setOutputSizeInBytes(other.getOutputSizeInBytes());
    }

    public static final class Builder
    {
        private double outputRowCount = NaN;
        private double outputSizeInBytes = NaN;

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setOutputSizeInBytes(double outputSizeInBytes)
        {
            this.outputSizeInBytes = outputSizeInBytes;
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            if (isNaN(outputSizeInBytes) && !isNaN(outputRowCount)) {
                outputSizeInBytes = DEFAULT_ROW_SIZE * outputRowCount;
            }
            return new PlanNodeStatsEstimate(outputRowCount, outputSizeInBytes);
        }
    }
}
