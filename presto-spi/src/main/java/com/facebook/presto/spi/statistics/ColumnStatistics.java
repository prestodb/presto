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

package com.facebook.presto.spi.statistics;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Objects.requireNonNull;

public final class ColumnStatistics
{
    private final Estimate dataSize;
    private final Estimate nullsFraction;
    private final Estimate distinctValuesCount;

    private ColumnStatistics(Estimate dataSize, Estimate nullsFraction, Estimate distinctValuesCount)
    {
        this.dataSize = requireNonNull(dataSize, "dataSize can not be null");
        this.nullsFraction = requireNonNull(nullsFraction, "nullsFraction can not be null");
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount can not be null");
    }

    public Estimate getDataSize()
    {
        return dataSize;
    }

    public Estimate getNullsFraction()
    {
        return nullsFraction;
    }

    public Estimate getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate dataSize = unknownValue();
        private Estimate nullsFraction = unknownValue();
        private Estimate distinctValuesCount = unknownValue();

        public Builder setDataSize(Estimate dataSize)
        {
            this.dataSize = dataSize;
            return this;
        }

        public Builder setNullsFraction(Estimate nullsFraction)
        {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public ColumnStatistics build()
        {
            return new ColumnStatistics(dataSize, nullsFraction, distinctValuesCount);
        }
    }
}
