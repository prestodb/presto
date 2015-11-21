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
package com.facebook.presto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class PartitionedPagePartitionFunction
        implements PagePartitionFunction
{
    protected final int partition;
    protected final int partitionCount;

    @JsonCreator
    public PartitionedPagePartitionFunction(
            @JsonProperty("partition") int partition,
            @JsonProperty("partitionCount") int partitionCount)
    {
        checkArgument(partition < partitionCount, "partition must be less than partitionCount");

        this.partition = partition;
        this.partitionCount = partitionCount;
    }

    @Override
    @JsonProperty
    public int getPartition()
    {
        return partition;
    }

    @Override
    @JsonProperty
    public int getPartitionCount()
    {
        return partitionCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partition, partitionCount);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PartitionedPagePartitionFunction other = (PartitionedPagePartitionFunction) obj;
        return Objects.equals(this.partition, other.partition) &&
                Objects.equals(this.partitionCount, other.partitionCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition", partition)
                .add("partitionCount", partitionCount)
                .toString();
    }
}
