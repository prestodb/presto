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
package com.facebook.presto.hive.util;

import com.google.common.base.Optional;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class DirectoryEntry
{
    private final FileStatus fileStatus;
    private final Optional<BlockLocation[]> blockLocations;

    public static DirectoryEntry entryForFile(FileStatus fileStatus, BlockLocation[] blockLocation)
    {
        checkNotNull(fileStatus, "fileStatus is null");
        checkNotNull(blockLocation, "blockLocations is null");
        return new DirectoryEntry(fileStatus, Optional.of(blockLocation));
    }

    public static DirectoryEntry entryForDirectory(FileStatus fileStatus)
    {
        checkNotNull(fileStatus, "fileStatus is null");
        return new DirectoryEntry(fileStatus, Optional.<BlockLocation[]>absent());
    }

    private DirectoryEntry(FileStatus fileStatus, Optional<BlockLocation[]> blockLocations)
    {
        this.fileStatus = checkNotNull(fileStatus, "fileStatus is null");
        this.blockLocations = checkNotNull(blockLocations, "blockLocations is null");
    }

    public FileStatus getFileStatus()
    {
        return fileStatus;
    }

    public BlockLocation[] getBlockLocations()
    {
        checkState(blockLocations.isPresent(), "%s is a directory", fileStatus.getPath());
        return blockLocations.get();
    }

    public boolean isDirectory()
    {
        return !blockLocations.isPresent();
    }
}
