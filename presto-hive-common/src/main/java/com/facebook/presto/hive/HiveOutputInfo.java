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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class HiveOutputInfo
{
    private final List<String> partitionNames;
    private final String tableLocation;

    @JsonCreator
    public HiveOutputInfo(
            @JsonProperty("partitionNames") List<String> partitionNames,
            @JsonProperty("tableLocation") String tableLocation)
    {
        this.partitionNames = ImmutableList.copyOf(requireNonNull(partitionNames, "partitionNames is null"));
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
    }

    @JsonProperty
    public List<String> getPartitionNames()
    {
        return partitionNames;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiveOutputInfo that = (HiveOutputInfo) o;
        return Objects.equals(partitionNames, that.partitionNames) && Objects.equals(tableLocation, that.tableLocation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionNames, tableLocation);
    }

    @Override
    public String toString()
    {
        return "HiveOutputInfo{" +
                "partitionNames=" + partitionNames +
                ", tableLocation='" + tableLocation + '\'' +
                '}';
    }
}
