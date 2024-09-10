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

import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.Estimate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.hash;

/**
 * This {@link ConnectorHistogram} implementation returns values assuming a
 * uniform distribution between a given high and low value.
 * <br>
 * In the case that statistics don't exist for a particular table, the Presto
 * optimizer will fall back on this uniform distribution assumption.
 */
public class UniformDistributionHistogram
        implements ConnectorHistogram
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(UniformDistributionHistogram.class).instanceSize();
    private final double lowValue;
    private final double highValue;

    @JsonCreator
    public UniformDistributionHistogram(
            @JsonProperty("lowValue") double lowValue,
            @JsonProperty("highValue") double highValue)
    {
        verify(isNaN(lowValue) || isNaN(highValue) || (lowValue <= highValue), "lowValue must be <= highValue");
        this.lowValue = lowValue;
        this.highValue = highValue;
    }

    @JsonProperty
    public double getLowValue()
    {
        return lowValue;
    }

    @JsonProperty
    public double getHighValue()
    {
        return highValue;
    }

    @Override
    public Estimate cumulativeProbability(double value, boolean inclusive)
    {
        if (isNaN(lowValue) ||
                isNaN(highValue) ||
                isNaN(value)) {
            return Estimate.unknown();
        }

        if (value >= highValue) {
            return Estimate.of(1.0);
        }

        if (value <= lowValue) {
            return Estimate.of(0.0);
        }

        if (isInfinite(lowValue) || isInfinite(highValue)) {
            return Estimate.unknown();
        }

        return Estimate.of(min(1.0, max(0.0, ((value - lowValue) / (highValue - lowValue)))));
    }

    @Override
    public Estimate inverseCumulativeProbability(double percentile)
    {
        checkArgument(percentile >= 0.0 && percentile <= 1.0, "percentile must be in [0.0, 1.0]: " + percentile);
        if (isNaN(lowValue) ||
                isNaN(highValue)) {
            return Estimate.unknown();
        }

        if (percentile == 0.0 && !isInfinite(lowValue)) {
            return Estimate.of(lowValue);
        }

        if (percentile == 1.0 && !isInfinite(highValue)) {
            return Estimate.of(highValue);
        }

        if (isInfinite(lowValue) || isInfinite(highValue)) {
            return Estimate.unknown();
        }

        return Estimate.of(lowValue + (percentile * (highValue - lowValue)));
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("lowValue", lowValue)
                .add("highValue", highValue)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UniformDistributionHistogram)) {
            return false;
        }

        UniformDistributionHistogram other = (UniformDistributionHistogram) o;
        return equalsOrBothNaN(lowValue, other.lowValue) &&
                equalsOrBothNaN(highValue, other.highValue);
    }

    @Override
    public int hashCode()
    {
        return hash(lowValue, highValue);
    }

    private static boolean equalsOrBothNaN(Double first, Double second)
    {
        return first.equals(second) || (Double.isNaN(first) && Double.isNaN(second));
    }
}
