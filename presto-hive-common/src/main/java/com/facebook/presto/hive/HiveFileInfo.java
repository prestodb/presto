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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveFileInfo
        implements Comparable
{
    private final Path path;
    private final boolean isDirectory;
    private final BlockLocation[] blockLocations;
    private final long length;
    private final long fileModifiedTime;
    private final Optional<byte[]> extraFileInfo;

    public static HiveFileInfo createHiveFileInfo(LocatedFileStatus locatedFileStatus, Optional<byte[]> extraFileContext)
    {
        return new HiveFileInfo(
                locatedFileStatus.getPath(),
                locatedFileStatus.isDirectory(),
                locatedFileStatus.getBlockLocations(),
                locatedFileStatus.getLen(),
                locatedFileStatus.getModificationTime(),
                extraFileContext);
    }

    private HiveFileInfo(Path path, boolean isDirectory, BlockLocation[] blockLocations, long length, long fileModifiedTime, Optional<byte[]> extraFileInfo)
    {
        this.path = requireNonNull(path, "path is null");
        this.isDirectory = isDirectory;
        this.blockLocations = blockLocations;
        this.length = length;
        this.fileModifiedTime = fileModifiedTime;
        this.extraFileInfo = requireNonNull(extraFileInfo, "extraFileInfo is null");
    }

    public Path getPath()
    {
        return path;
    }

    public boolean isDirectory()
    {
        return isDirectory;
    }

    public BlockLocation[] getBlockLocations()
    {
        return blockLocations;
    }

    public long getLength()
    {
        return length;
    }

    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    public Optional<byte[]> getExtraFileInfo()
    {
        return extraFileInfo;
    }

    @Override
    public int compareTo(Object o)
    {
        HiveFileInfo other = (HiveFileInfo) o;
        return this.getPath().compareTo(other.getPath());
    }
}
