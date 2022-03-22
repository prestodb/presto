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
package com.facebook.presto.orc.metadata.statistics;

import com.google.common.base.MoreObjects.ToStringHelper;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.facebook.presto.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static java.util.Objects.requireNonNull;

public class BinaryColumnStatistics
        extends ColumnStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BinaryColumnStatistics.class).instanceSize();

    private final BinaryStatistics binaryStatistics;

    public BinaryColumnStatistics(
            Long numberOfValues,
            HiveBloomFilter bloomFilter,
            BinaryStatistics binaryStatistics)
    {
        super(numberOfValues, bloomFilter);
        requireNonNull(binaryStatistics, "binaryStatistics is null");
        this.binaryStatistics = binaryStatistics;
    }

    @Override
    public BinaryStatistics getBinaryStatistics()
    {
        return binaryStatistics;
    }

    @Override
    public long getMinAverageValueSizeInBytes()
    {
        long numberOfValues = getNumberOfValues();
        return BINARY_VALUE_BYTES_OVERHEAD + (numberOfValues > 0 ? binaryStatistics.getSum() / numberOfValues : 0);
    }

    @Override
    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new BinaryColumnStatistics(
                getNumberOfValues(),
                bloomFilter,
                binaryStatistics);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE + super.getMembersSizeInBytes();
        return sizeInBytes + binaryStatistics.getRetainedSizeInBytes();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        super.addHash(hasher);
        hasher.putOptionalHashable(binaryStatistics);
    }

    @Override
    protected ToStringHelper getToStringHelper()
    {
        return super.getToStringHelper()
                .add("binaryStatistics", binaryStatistics);
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
        BinaryColumnStatistics that = (BinaryColumnStatistics) o;
        return equalsInternal(that) && Objects.equals(binaryStatistics, that.binaryStatistics);
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), binaryStatistics);
    }
}
