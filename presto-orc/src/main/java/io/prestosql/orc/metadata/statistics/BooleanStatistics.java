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
package io.prestosql.orc.metadata.statistics;

import io.prestosql.orc.metadata.statistics.StatisticsHasher.Hashable;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BooleanStatistics
        implements Hashable
{
    // 1 byte to denote if null + 1 byte for the value
    public static final long BOOLEAN_VALUE_BYTES = Byte.BYTES + Byte.BYTES;

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanStatistics.class).instanceSize();

    private final long trueValueCount;

    public BooleanStatistics(long trueValueCount)
    {
        this.trueValueCount = trueValueCount;
    }

    public long getTrueValueCount()
    {
        return trueValueCount;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
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
        BooleanStatistics that = (BooleanStatistics) o;
        return trueValueCount == that.trueValueCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(trueValueCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("trueValueCount", trueValueCount)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putLong(trueValueCount);
    }
}
