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
    // V3 deletion-vector-only fields. Empty for V2 position-delete commits;
    // populated for V3 commits where the page sink (Velox or Java fallback)
    // writes a Puffin blob alongside the data file.
    private final Optional<Long> contentOffset;
    private final Optional<Long> contentSizeInBytes;

    public CommitTaskData(
            String path,
            long fileSizeInBytes,
            MetricsWrapper metrics,
            int partitionSpecId,
            Optional<String> partitionDataJson,
            FileFormat fileFormat,
            String referencedDataFile,
            FileContent content)
    {
        this(
                path,
                fileSizeInBytes,
                metrics,
                partitionSpecId,
                partitionDataJson,
                fileFormat,
                referencedDataFile,
                content,
                Optional.empty(),
                Optional.empty());
    }

    @JsonCreator
    public CommitTaskData(
            @JsonProperty("path") String path,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("metrics") MetricsWrapper metrics,
            @JsonProperty("partitionSpecJson") int partitionSpecId,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("referencedDataFile") String referencedDataFile,
            @JsonProperty("content") FileContent content,
            @JsonProperty("contentOffset") Optional<Long> contentOffset,
            @JsonProperty("contentSizeInBytes") Optional<Long> contentSizeInBytes)
    {
        this.path = requireNonNull(path, "path is null");
        this.fileSizeInBytes = fileSizeInBytes;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.partitionSpecId = partitionSpecId;
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.referencedDataFile = Optional.ofNullable(referencedDataFile);
        this.content = requireNonNull(content, "content is null");
        // Tolerate missing fields on JSON deserialization so V2 fragments produced
        // by older binaries continue to round-trip during the deploy window.
        this.contentOffset = contentOffset == null ? Optional.empty() : contentOffset;
        this.contentSizeInBytes = contentSizeInBytes == null ? Optional.empty() : contentSizeInBytes;
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

    @JsonProperty
    public Optional<Long> getContentOffset()
    {
        return contentOffset;
    }

    @JsonProperty
    public Optional<Long> getContentSizeInBytes()
    {
        return contentSizeInBytes;
    }
}
