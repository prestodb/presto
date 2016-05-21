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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionUpdate
{
    private final String name;
    private final boolean isNew;
    private final Path writePath;
    private final Path targetPath;
    private final List<String> fileNames;

    public PartitionUpdate(
            @JsonProperty("name") String name,
            @JsonProperty("new") boolean isNew,
            @JsonProperty("writePath") String writePath,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("fileNames") List<String> fileNames)
    {
        this.name = requireNonNull(name, "name is null");
        this.isNew = isNew;
        this.writePath = new Path(requireNonNull(writePath, "writePath is null"));
        this.targetPath = new Path(requireNonNull(targetPath, "targetPath is null"));
        this.fileNames = ImmutableList.copyOf(requireNonNull(fileNames, "fileNames is null"));
    }

    public PartitionUpdate(String name, boolean isNew, Path writePath, Path targetPath, List<String> fileNames)
    {
        this.name = requireNonNull(name, "name is null");
        this.isNew = isNew;
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.fileNames = ImmutableList.copyOf(requireNonNull(fileNames, "fileNames is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isNew()
    {
        return isNew;
    }

    public Path getWritePath()
    {
        return writePath;
    }

    public Path getTargetPath()
    {
        return targetPath;
    }

    @JsonProperty
    public List<String> getFileNames()
    {
        return fileNames;
    }

    @JsonProperty("targetPath")
    public String getJsonSerializableTargetPath()
    {
        return targetPath.toString();
    }

    @JsonProperty("writePath")
    public String getJsonSerializableWritePath()
    {
        return writePath.toString();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .toString();
    }

    public static List<PartitionUpdate> mergePartitionUpdates(List<PartitionUpdate> unMergedUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdates = ImmutableList.builder();
        for (Collection<PartitionUpdate> partitionGroup : Multimaps.index(unMergedUpdates, PartitionUpdate::getName).asMap().values()) {
            PartitionUpdate firstPartition = partitionGroup.iterator().next();

            ImmutableList.Builder<String> allFileNames = ImmutableList.builder();
            for (PartitionUpdate partition : partitionGroup) {
                // verify partitions have the same new flag, write path and target path
                // this shouldn't happen but could if another user added a partition during the write
                if (partition.isNew() != firstPartition.isNew() ||
                        !partition.getWritePath().equals(firstPartition.getWritePath()) ||
                        !partition.getTargetPath().equals(firstPartition.getTargetPath())) {
                    throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, format("Partition %s was added or modified during INSERT", firstPartition.getName()));
                }
                allFileNames.addAll(partition.getFileNames());
            }

            partitionUpdates.add(new PartitionUpdate(firstPartition.getName(),
                    firstPartition.isNew(),
                    firstPartition.getWritePath(),
                    firstPartition.getTargetPath(),
                    allFileNames.build()));
        }
        return partitionUpdates.build();
    }
}
