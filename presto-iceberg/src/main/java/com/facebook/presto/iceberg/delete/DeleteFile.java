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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeleteFile
{
    private final FileContent content;
    private final String path;
    private final FileFormat format;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final List<Integer> equalityFieldIds;
    private final Map<Integer, ByteBuffer> lowerBounds;
    private final Map<Integer, ByteBuffer> upperBounds;

    public static DeleteFile fromIceberg(org.apache.iceberg.DeleteFile deleteFile)
    {
        return new DeleteFile(
                deleteFile.content(),
                deleteFile.path().toString(),
                deleteFile.format(),
                deleteFile.recordCount(),
                deleteFile.fileSizeInBytes(),
                Optional.ofNullable(deleteFile.equalityFieldIds()).orElseGet(ImmutableList::of),
                deleteFile.lowerBounds(),
                deleteFile.upperBounds());
    }

    @JsonCreator
    public DeleteFile(
            FileContent content,
            String path,
            FileFormat format,
            long recordCount,
            long fileSizeInBytes,
            List<Integer> equalityFieldIds,
            Map<Integer, ByteBuffer> lowerBounds,
            Map<Integer, ByteBuffer> upperBounds)
    {
        this.content = requireNonNull(content, "content is null");
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.equalityFieldIds = ImmutableList.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
        this.lowerBounds = requireNonNull(lowerBounds, "lowerBounds is null");
        this.upperBounds = requireNonNull(upperBounds, "upperBounds is null");
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
    public Map<Integer, ByteBuffer> getLowerBounds()
    {
        return lowerBounds;
    }

    @JsonProperty
    public Map<Integer, ByteBuffer> getUpperBounds()
    {
        return upperBounds;
    }
}
