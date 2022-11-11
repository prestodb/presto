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
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableMap;

import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HiveFileSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileModifiedTime;
    private final Optional<byte[]> extraFileInfo;
    private final Map<String, String> customSplitInfo;

    private AtomicReference<String> cachedToString = new AtomicReference<>();

    @JsonCreator
    public HiveFileSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("extraFileInfo") Optional<byte[]> extraFileInfo,
            @JsonProperty("customSplitInfo") Map<String, String> customSplitInfo)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(fileSize >= 0, "fileSize must be positive");
        checkArgument(fileModifiedTime >= 0, "modificationTime must be positive");
        requireNonNull(path, "path is null");
        requireNonNull(extraFileInfo, "extraFileInfo is null");
        requireNonNull(customSplitInfo, "customSplitInfo is null");

        this.path = path;
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.extraFileInfo = extraFileInfo;
        this.customSplitInfo = ImmutableMap.copyOf(customSplitInfo);
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty
    public Optional<byte[]> getExtraFileInfo()
    {
        return extraFileInfo;
    }

    @JsonProperty
    public Map<String, String> getCustomSplitInfo()
    {
        return customSplitInfo;
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

        HiveFileSplit fileSplit = (HiveFileSplit) o;
        return start == fileSplit.start
                && length == fileSplit.length
                && fileSize == fileSplit.fileSize
                && fileModifiedTime == fileSplit.fileModifiedTime
                && Objects.equals(path, fileSplit.path)
                && Objects.equals(extraFileInfo, fileSplit.extraFileInfo)
                && Objects.equals(customSplitInfo, fileSplit.customSplitInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, start, length, fileSize, fileModifiedTime, extraFileInfo, customSplitInfo);
    }

    @Override
    public String toString()
    {
        String existingToString = cachedToString.get();
        if (existingToString != null) {
            return existingToString;
        }

        ToStringHelper toStringHelper = toStringHelper(this);
        toStringHelper.add("path", path);
        toStringHelper.add("start", start);
        toStringHelper.add("length", length);
        toStringHelper.add("fileSize", fileSize);
        toStringHelper.add("fileModifiedTime", fileModifiedTime);

        if (extraFileInfo.isPresent()) {
            String extraFileInfoString = new String(Base64.getEncoder().encode(extraFileInfo.get()));
            toStringHelper.add("extraFileInfo", extraFileInfoString);
        }

        if (!customSplitInfo.isEmpty()) {
            toStringHelper.add("customSplitInfo", customSplitInfo);
        }

        cachedToString.compareAndSet(null, toStringHelper.toString());
        return cachedToString.get();
    }
}
