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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.BlockLocation.fromHiveBlockLocations;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class HiveFileInfo
        implements Comparable
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(HiveFileInfo.class).instanceSize();

    private final String path;
    private final boolean isDirectory;
    private final List<BlockLocation> blockLocations;
    private final long length;
    private final long fileModifiedTime;
    private final Optional<byte[]> extraFileInfo;
    private final Map<String, String> customSplitInfo;

    public static HiveFileInfo createHiveFileInfo(LocatedFileStatus locatedFileStatus, Optional<byte[]> extraFileContext)
            throws IOException
    {
        return createHiveFileInfo(
                locatedFileStatus,
                extraFileContext,
                ImmutableMap.of());
    }

    public static HiveFileInfo createHiveFileInfo(LocatedFileStatus locatedFileStatus, Optional<byte[]> extraFileContext, Map<String, String> customSplitInfo)
            throws IOException
    {
        return new HiveFileInfo(
                locatedFileStatus.getPath().toString(),
                locatedFileStatus.isDirectory(),
                fromHiveBlockLocations(locatedFileStatus.getBlockLocations()),
                locatedFileStatus.getLen(),
                locatedFileStatus.getModificationTime(),
                extraFileContext,
                customSplitInfo);
    }

    @ThriftConstructor
    public HiveFileInfo(
            String path,
            boolean directory,
            List<BlockLocation> blockLocations,
            long length,
            long fileModifiedTime,
            Optional<byte[]> extraFileInfo,
            Map<String, String> customSplitInfo)
    {
        this.path = requireNonNull(path, "path is null");
        this.isDirectory = directory;
        this.blockLocations = requireNonNull(blockLocations, "blockLocations is null");
        this.length = length;
        this.fileModifiedTime = fileModifiedTime;
        this.extraFileInfo = requireNonNull(extraFileInfo, "extraFileInfo is null");
        this.customSplitInfo = requireNonNull(customSplitInfo, "customSplitInfo is null");
    }

    @ThriftField(1)
    public String getPath()
    {
        return path;
    }

    @ThriftField(2)
    public boolean isDirectory()
    {
        return isDirectory;
    }

    @ThriftField(3)
    public List<BlockLocation> getBlockLocations()
    {
        return blockLocations;
    }

    @ThriftField(4)
    public long getLength()
    {
        return length;
    }

    @ThriftField(5)
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @ThriftField(6)
    public Optional<byte[]> getExtraFileInfo()
    {
        return extraFileInfo;
    }

    @ThriftField(7)
    public Map<String, String> getCustomSplitInfo()
    {
        return customSplitInfo;
    }

    public long getRetainedSizeInBytes()
    {
        long blockLocationsSizeInBytes = blockLocations.stream().map(BlockLocation::getRetainedSizeInBytes).reduce(0L, Long::sum);
        long extraFileInfoSizeInBytes = extraFileInfo.map(bytes -> bytes.length).orElse(0);
        long customSplitInfoSizeInBytes = customSplitInfo.entrySet().stream().mapToLong(e -> e.getKey().length() + e.getValue().length()).reduce(0, Long::sum);
        return INSTANCE_SIZE + path.length() + blockLocationsSizeInBytes + extraFileInfoSizeInBytes + customSplitInfoSizeInBytes;
    }

    public String getParent()
    {
        return path.substring(0, path.lastIndexOf('/'));
    }

    public String getFileName()
    {
        return path.substring(path.lastIndexOf('/') + 1);
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
        HiveFileInfo that = (HiveFileInfo) o;
        return path.equals(that.path) &&
                blockLocations.equals(that.blockLocations) &&
                length == that.length;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, blockLocations, length);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("isDirectory", isDirectory)
                .add("blockLocations", blockLocations)
                .add("length", length)
                .add("fileModifiedTime", fileModifiedTime)
                .add("customSplitInfo", customSplitInfo)
                .toString();
    }

    @Override
    public int compareTo(Object o)
    {
        HiveFileInfo other = (HiveFileInfo) o;
        return this.getPath().compareTo(other.getPath());
    }
}
