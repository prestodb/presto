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
package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ComparisonChain;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class PartitionNameWithVersion
        implements Comparable<PartitionNameWithVersion>
{
    private final String partitionName;
    private final Optional<Long> partitionVersion;

    @JsonCreator
    public PartitionNameWithVersion(@JsonProperty("partitionName") String partitionName, @JsonProperty("partitionVersion") Optional<Long> partitionVersion)
    {
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.partitionVersion = requireNonNull(partitionVersion, "partitionVersion is null");
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public Optional<Long> getPartitionVersion()
    {
        return partitionVersion;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("PartitionNameWithVersion{");
        sb.append("partitionName='").append(partitionName).append('\'');
        sb.append(", partitionVersion=").append(partitionVersion);
        sb.append('}');
        return sb.toString();
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
        PartitionNameWithVersion that = (PartitionNameWithVersion) o;
        return Objects.equals(partitionName, that.partitionName) && Objects.equals(partitionVersion, that.partitionVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionName, partitionVersion);
    }

    @Override
    public int compareTo(PartitionNameWithVersion other)
    {
        return ComparisonChain.start()
                .compare(this.partitionName, other.partitionName)
                .compare(this.partitionVersion.orElse(Long.MIN_VALUE), other.partitionVersion.orElse(Long.MIN_VALUE))
                .result();
    }
}
