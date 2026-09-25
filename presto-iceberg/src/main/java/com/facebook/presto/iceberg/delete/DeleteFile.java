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
package com.facebook.presto.iceberg.delete;

import com.facebook.presto.iceberg.FileContent;
import com.facebook.presto.iceberg.FileFormat;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.FileContent.fromIcebergFileContent;
import static com.facebook.presto.iceberg.FileFormat.fromIcebergFileFormat;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public final class DeleteFile
{
    private final FileContent content;
    private final String path;
    private final FileFormat format;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final List<Integer> equalityFieldIds;
    private final Map<Integer, byte[]> lowerBounds;
    private final Map<Integer, byte[]> upperBounds;
    // Iceberg V3 deletion vector (Puffin blob) coordinates. Absent for V2
    // positional / equality delete files. `contentOffset` /
    // `contentSizeInBytes` locate the DV blob within the Puffin file;
    // `referencedDataFile` names the single data file the DV applies to.
    private final Optional<Long> contentOffset;
    private final Optional<Long> contentSizeInBytes;
    private final Optional<String> referencedDataFile;
    // Iceberg sequence number of the delete file. The Velox-side DV applier
    // uses this to skip DVs whose sequence number is <= the data file's.
    private final long dataSequenceNumber;

    public static DeleteFile fromIceberg(org.apache.iceberg.DeleteFile deleteFile)
    {
        Map<Integer, byte[]> lowerBounds = firstNonNull(deleteFile.lowerBounds(), ImmutableMap.<Integer, ByteBuffer>of())
                .entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().array().clone()));
        Map<Integer, byte[]> upperBounds = firstNonNull(deleteFile.upperBounds(), ImmutableMap.<Integer, ByteBuffer>of())
                .entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().array().clone()));

        return new DeleteFile(
                fromIcebergFileContent(deleteFile.content()),
                deleteFile.path().toString(),
                fromIcebergFileFormat(deleteFile.format()),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                Optional.ofNullable(deleteFile.equalityFieldIds()).orElseGet(ImmutableList::of),
                lowerBounds,
                upperBounds,
                Optional.ofNullable(deleteFile.contentOffset()),
                Optional.ofNullable(deleteFile.contentSizeInBytes()),
                Optional.ofNullable(deleteFile.referencedDataFile()),
                deleteFile.dataSequenceNumber());
    }

    @JsonCreator
    public DeleteFile(
            @JsonProperty("content") FileContent content,
            @JsonProperty("path") String path,
            @JsonProperty("format") FileFormat format,
            @JsonProperty("recordCount") long recordCount,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("equalityFieldIds") List<Integer> equalityFieldIds,
            @JsonProperty("lowerBounds") Map<Integer, byte[]> lowerBounds,
            @JsonProperty("upperBounds") Map<Integer, byte[]> upperBounds,
            @JsonProperty("contentOffset") Optional<Long> contentOffset,
            @JsonProperty("contentSizeInBytes") Optional<Long> contentSizeInBytes,
            @JsonProperty("referencedDataFile") Optional<String> referencedDataFile,
            @JsonProperty("dataSequenceNumber") long dataSequenceNumber)
    {
        this.content = requireNonNull(content, "content is null");
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.equalityFieldIds = ImmutableList.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
        this.lowerBounds = ImmutableMap.copyOf(requireNonNull(lowerBounds, "lowerBounds is null"));
        this.upperBounds = ImmutableMap.copyOf(requireNonNull(upperBounds, "upperBounds is null"));
        this.contentOffset = requireNonNull(contentOffset, "contentOffset is null");
        this.contentSizeInBytes = requireNonNull(contentSizeInBytes, "contentSizeInBytes is null");
        this.referencedDataFile = requireNonNull(referencedDataFile, "referencedDataFile is null");
        this.dataSequenceNumber = dataSequenceNumber;
    }

    @JsonProperty
    public FileContent content()
    {
        return content;
    }

    @JsonProperty
    public String path()
    {
        return path;
    }

    @JsonProperty
    public FileFormat format()
    {
        return format;
    }

    @JsonProperty
    public long recordCount()
    {
        return recordCount;
    }

    @JsonProperty
    public long fileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public List<Integer> equalityFieldIds()
    {
        return equalityFieldIds;
    }

    @JsonProperty
    public Map<Integer, byte[]> getLowerBounds()
    {
        return lowerBounds;
    }

    @JsonProperty
    public Map<Integer, byte[]> getUpperBounds()
    {
        return upperBounds;
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

    @JsonProperty
    public Optional<String> getReferencedDataFile()
    {
        return referencedDataFile;
    }

    @JsonProperty
    public long getDataSequenceNumber()
    {
        return dataSequenceNumber;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .add("records", recordCount)
                .toString();
    }
}
