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
package io.prestosql.plugin.cassandra;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SizeEstimate
{
    private final String rangeStart;
    private final String rangeEnd;
    private final long meanPartitionSize;
    private final long partitionsCount;

    public SizeEstimate(String rangeStart, String rangeEnd, long meanPartitionSize, long partitionsCount)
    {
        this.rangeStart = requireNonNull(rangeStart, "rangeStart is null");
        this.rangeEnd = requireNonNull(rangeEnd, "rangeEnd is null");
        this.meanPartitionSize = meanPartitionSize;
        this.partitionsCount = partitionsCount;
    }

    public String getRangeStart()
    {
        return rangeStart;
    }

    public String getRangeEnd()
    {
        return rangeEnd;
    }

    public long getMeanPartitionSize()
    {
        return meanPartitionSize;
    }

    public long getPartitionsCount()
    {
        return partitionsCount;
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
        SizeEstimate that = (SizeEstimate) o;
        return meanPartitionSize == that.meanPartitionSize &&
                partitionsCount == that.partitionsCount &&
                Objects.equals(rangeStart, that.rangeStart) &&
                Objects.equals(rangeEnd, that.rangeEnd);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rangeStart, rangeEnd, meanPartitionSize, partitionsCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rangeStart", rangeStart)
                .add("rangeEnd", rangeEnd)
                .add("meanPartitionSize", meanPartitionSize)
                .add("partitionsCount", partitionsCount)
                .toString();
    }
}
