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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MemorySplit
        implements ConnectorSplit
{
    private final MemoryTableHandle tableHandle;
    private final int totalPartsPerWorker; // how many concurrent reads there will be from one worker
    private final int partNumber; // part of the pages on one worker that this splits is responsible
    private final List<HostAddress> addresses;

    @JsonCreator
    public MemorySplit(
            @JsonProperty("tableHandle") MemoryTableHandle tableHandle,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalPartsPerWorker") int totalPartsPerWorker,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalPartsPerWorker >= 1, "totalPartsPerWorker must be >= 1");
        checkState(totalPartsPerWorker > partNumber, "totalPartsPerWorker must be > partNumber");

        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partNumber = partNumber;
        this.totalPartsPerWorker = totalPartsPerWorker;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public MemoryTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public int getTotalPartsPerWorker()
    {
        return totalPartsPerWorker;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MemorySplit other = (MemorySplit) obj;
        return Objects.equals(this.tableHandle, other.tableHandle) &&
                Objects.equals(this.totalPartsPerWorker, other.totalPartsPerWorker) &&
                Objects.equals(this.partNumber, other.partNumber);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, totalPartsPerWorker, partNumber);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("partNumber", partNumber)
                .add("totalPartsPerWorker", totalPartsPerWorker)
                .toString();
    }
}
