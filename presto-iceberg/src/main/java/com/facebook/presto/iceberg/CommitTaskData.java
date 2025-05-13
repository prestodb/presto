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
    private final long fileSizeInBytes;
    private final MetricsWrapper metrics;
    private final int partitionSpecId;
    private final Optional<String> partitionDataJson;
    private final FileFormat fileFormat;
    private final Optional<String> referencedDataFile;
    private final FileContent content;

    @JsonCreator
    public CommitTaskData(
            @JsonProperty("path") String path,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("metrics") MetricsWrapper metrics,
            @JsonProperty("partitionSpecJson") int partitionSpecId,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("referencedDataFile") String referencedDataFile,
            @JsonProperty("content") FileContent content)
    {
        this.path = requireNonNull(path, "path is null");
        this.fileSizeInBytes = fileSizeInBytes;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.partitionSpecId = partitionSpecId;
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.referencedDataFile = Optional.ofNullable(referencedDataFile);
        this.content = requireNonNull(content, "content is null");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public MetricsWrapper getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public int getPartitionSpecId()
    {
        return partitionSpecId;
    }

    @JsonProperty
    public Optional<String> getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Optional<String> getReferencedDataFile()
    {
        return referencedDataFile;
    }

    @JsonProperty
    public FileContent getContent()
    {
        return content;
    }
}
