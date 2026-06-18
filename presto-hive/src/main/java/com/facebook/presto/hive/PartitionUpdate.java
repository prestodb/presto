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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import org.apache.hadoop.fs.Path;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionUpdate
{
    private final String name;
    private final UpdateMode updateMode;
    private final Path writePath;
    private final Path targetPath;
    private final List<FileWriteInfo> fileWriteInfos;
    private final long rowCount;
    private final long inMemoryDataSizeInBytes;
    private final long onDiskDataSizeInBytes;
    private final boolean containsNumberedFileNames;

    @JsonCreator
    public PartitionUpdate(
            @JsonProperty("name") String name,
            @JsonProperty("updateMode") UpdateMode updateMode,
            @JsonProperty("writePath") String writePath,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("fileWriteInfos") List<FileWriteInfo> fileWriteInfos,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("inMemoryDataSizeInBytes") long inMemoryDataSizeInBytes,
            @JsonProperty("onDiskDataSizeInBytes") long onDiskDataSizeInBytes,
            @JsonProperty("containsNumberedFileNames") boolean containsNumberedFileNames)
    {
        this(
                name,
                updateMode,
                new Path(requireNonNull(writePath, "writePath is null")),
                new Path(requireNonNull(targetPath, "targetPath is null")),
                fileWriteInfos,
                rowCount,
                inMemoryDataSizeInBytes,
                onDiskDataSizeInBytes,
                containsNumberedFileNames);
    }

    public PartitionUpdate(
            String name,
            UpdateMode updateMode,
            Path writePath,
            Path targetPath,
            List<FileWriteInfo> fileWriteInfos,
            long rowCount,
            long inMemoryDataSizeInBytes,
            long onDiskDataSizeInBytes,
            boolean containsNumberedFileNames)
    {
        this.name = requireNonNull(name, "name is null");
        this.updateMode = requireNonNull(updateMode, "updateMode is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.fileWriteInfos = ImmutableList.copyOf(requireNonNull(fileWriteInfos, "fileWriteInfos is null"));
        checkArgument(rowCount >= 0, "rowCount is negative: %d", rowCount);
        this.rowCount = rowCount;
        checkArgument(inMemoryDataSizeInBytes >= 0, "inMemoryDataSizeInBytes is negative: %d", inMemoryDataSizeInBytes);
        this.inMemoryDataSizeInBytes = inMemoryDataSizeInBytes;
        checkArgument(onDiskDataSizeInBytes >= 0, "onDiskDataSizeInBytes is negative: %d", onDiskDataSizeInBytes);
        this.onDiskDataSizeInBytes = onDiskDataSizeInBytes;
        this.containsNumberedFileNames = containsNumberedFileNames;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public UpdateMode getUpdateMode()
    {
        return updateMode;
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
    public List<FileWriteInfo> getFileWriteInfos()
    {
        return fileWriteInfos;
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

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public long getInMemoryDataSizeInBytes()
    {
        return inMemoryDataSizeInBytes;
    }

    @JsonProperty
    public long getOnDiskDataSizeInBytes()
    {
        return onDiskDataSizeInBytes;
    }

    @JsonProperty
    public boolean containsNumberedFileNames()
    {
        return containsNumberedFileNames;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("updateMode", updateMode)
                .add("writePath", writePath)
                .add("targetPath", targetPath)
                .add("fileWriteInfos", fileWriteInfos)
                .add("rowCount", rowCount)
                .add("inMemoryDataSizeInBytes", inMemoryDataSizeInBytes)
                .add("onDiskDataSizeInBytes", onDiskDataSizeInBytes)
                .add("containsNumberedFileNames", containsNumberedFileNames)
                .toString();
    }

    public HiveBasicStatistics getStatistics()
    {
        return new HiveBasicStatistics(fileWriteInfos.size(), rowCount, inMemoryDataSizeInBytes, onDiskDataSizeInBytes);
    }

    public static List<PartitionUpdate> mergePartitionUpdates(Iterable<PartitionUpdate> unMergedUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdates = ImmutableList.builder();
        for (Collection<PartitionUpdate> partitionGroup : Multimaps.index(unMergedUpdates, PartitionUpdate::getName).asMap().values()) {
            PartitionUpdate firstPartition = partitionGroup.iterator().next();

            ImmutableList.Builder<FileWriteInfo> allFileWriterInfos = ImmutableList.builder();
            long totalRowCount = 0;
            long totalInMemoryDataSizeInBytes = 0;
            long totalOnDiskDataSizeInBytes = 0;
            boolean containsNumberedFileNames = true;
            for (PartitionUpdate partition : partitionGroup) {
                // verify partitions have the same new flag, write path and target path
                // this shouldn't happen but could if another user added a partition during the write
                if (partition.getUpdateMode() != firstPartition.getUpdateMode() ||
                        !partition.getWritePath().equals(firstPartition.getWritePath()) ||
                        !partition.getTargetPath().equals(firstPartition.getTargetPath())) {
                    throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, format("Partition %s was added or modified during INSERT", firstPartition.getName()));
                }
                allFileWriterInfos.addAll(partition.getFileWriteInfos());
                totalRowCount += partition.getRowCount();
                totalInMemoryDataSizeInBytes += partition.getInMemoryDataSizeInBytes();
                totalOnDiskDataSizeInBytes += partition.getOnDiskDataSizeInBytes();
                containsNumberedFileNames &= partition.containsNumberedFileNames();
            }

            partitionUpdates.add(new PartitionUpdate(firstPartition.getName(),
                    firstPartition.getUpdateMode(),
                    firstPartition.getWritePath(),
                    firstPartition.getTargetPath(),
                    allFileWriterInfos.build(),
                    totalRowCount,
                    totalInMemoryDataSizeInBytes,
                    totalOnDiskDataSizeInBytes,
                    containsNumberedFileNames));
        }
        return partitionUpdates.build();
    }

    public enum UpdateMode
    {
        NEW,
        APPEND,
        OVERWRITE,
    }

    public static class FileWriteInfo
    {
        private final String writeFileName;
        private final String targetFileName;
        private final Optional<Long> fileSize;

        @JsonCreator
        public FileWriteInfo(
                @JsonProperty("writeFileName") String writeFileName,
                @JsonProperty("targetFileName") String targetFileName,
                @JsonProperty("fileSize") Optional<Long> fileSize)
        {
            this.writeFileName = requireNonNull(writeFileName, "writeFileName is null");
            this.targetFileName = requireNonNull(targetFileName, "targetFileName is null");
            this.fileSize = requireNonNull(fileSize, "fileSize is null");
        }

        @JsonProperty
        public String getWriteFileName()
        {
            return writeFileName;
        }

        @JsonProperty
        public String getTargetFileName()
        {
            return targetFileName;
        }

        @JsonProperty
        public Optional<Long> getFileSize()
        {
            return fileSize;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FileWriteInfo)) {
                return false;
            }
            FileWriteInfo that = (FileWriteInfo) o;
            return Objects.equals(writeFileName, that.writeFileName) &&
                    Objects.equals(targetFileName, that.targetFileName) &&
                    Objects.equals(fileSize, that.fileSize);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(writeFileName, targetFileName, fileSize);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("writeFileName", writeFileName)
                    .add("targetFileName", targetFileName)
                    .add("fileSize", fileSize)
                    .toString();
        }
    }
}
