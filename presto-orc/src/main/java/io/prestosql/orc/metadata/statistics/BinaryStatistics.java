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

import static com.google.common.base.MoreObjects.toStringHelper;

public class BinaryStatistics
        implements Hashable
{
    // 1 byte to denote if null + 4 bytes to denote offset
    public static final long BINARY_VALUE_BYTES_OVERHEAD = Byte.BYTES + Integer.BYTES;

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BinaryStatistics.class).instanceSize();

    private final long sum;

    public BinaryStatistics(long sum)
    {
        this.sum = sum;
    }

    public long getSum()
    {
        return sum;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sum", sum)
                .toString();
    }

    @Override
    public void addHash(StatisticsHasher hasher)
    {
        hasher.putLong(sum);
    }
}
