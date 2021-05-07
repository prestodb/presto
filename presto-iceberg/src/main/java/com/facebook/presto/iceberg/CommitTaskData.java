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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CommitTaskData
{
    private final String path;
    private final MetricsWrapper metrics;
    private final Optional<String> partitionDataJson;

    @JsonCreator
    public CommitTaskData(
            @JsonProperty("path") String path,
            @JsonProperty("metrics") MetricsWrapper metrics,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson)
    {
        this.path = requireNonNull(path, "path is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public MetricsWrapper getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public Optional<String> getPartitionDataJson()
    {
        return partitionDataJson;
    }
}
