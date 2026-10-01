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
package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryInfo
{
    private final String materializedViewName;
    private final MaterializedViewState freshnessState;
    private final int stalePartitionCount;
    private final List<String> baseTableNames;
    private final List<String> sampleStalePartitions;

    @JsonCreator
    public MaterializedViewQueryInfo(
            @JsonProperty("materializedViewName") String materializedViewName,
            @JsonProperty("freshnessState") MaterializedViewState freshnessState,
            @JsonProperty("stalePartitionCount") int stalePartitionCount,
            @JsonProperty("baseTableNames") List<String> baseTableNames,
            @JsonProperty("sampleStalePartitions") List<String> sampleStalePartitions)
    {
        this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        this.freshnessState = requireNonNull(freshnessState, "freshnessState is null");
        this.stalePartitionCount = stalePartitionCount;
        this.baseTableNames = requireNonNull(baseTableNames, "baseTableNames is null");
        this.sampleStalePartitions = requireNonNull(sampleStalePartitions, "sampleStalePartitions is null");
    }

    @JsonProperty
    public String getMaterializedViewName()
    {
        return materializedViewName;
    }

    @JsonProperty
    public MaterializedViewState getFreshnessState()
    {
        return freshnessState;
    }

    @JsonProperty
    public int getStalePartitionCount()
    {
        return stalePartitionCount;
    }

    @JsonProperty
    public List<String> getBaseTableNames()
    {
        return baseTableNames;
    }

    @JsonProperty
    public List<String> getSampleStalePartitions()
    {
        return sampleStalePartitions;
    }
}
