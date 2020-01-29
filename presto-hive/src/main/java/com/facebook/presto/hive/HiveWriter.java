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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import com.facebook.presto.hive.PartitionUpdate.UpdateMode;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.spi.Page;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveWriter
{
    private static final JsonCodec<FileStatistics> FILE_COLUMN_STATISTICS_JSON_CODEC = jsonCodec(FileStatistics.class);

    private final HiveFileWriter fileWriter;
    private final Optional<String> partitionName;
    private final UpdateMode updateMode;
    private final FileWriteInfo fileWriteInfo;
    private final String writePath;
    private final String targetPath;
    private final Consumer<HiveWriter> onCommit;
    private final HiveWriterStats hiveWriterStats;

    private long rowCount;
    private long inputSizeInBytes;
    private String fileStats = "";

    public HiveWriter(
            HiveFileWriter fileWriter,
            Optional<String> partitionName,
            UpdateMode updateMode,
            FileWriteInfo fileWriteInfo,
            String writePath,
            String targetPath,
            Consumer<HiveWriter> onCommit,
            HiveWriterStats hiveWriterStats)
    {
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.updateMode = requireNonNull(updateMode, "updateMode is null");
        this.fileWriteInfo = requireNonNull(fileWriteInfo, "fileWriteInfo is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.onCommit = requireNonNull(onCommit, "onCommit is null");
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
    }

    public long getWrittenBytes()
    {
        return fileWriter.getWrittenBytes();
    }

    public long getSystemMemoryUsage()
    {
        return fileWriter.getSystemMemoryUsage();
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public void append(Page dataPage)
    {
        // getRegionSizeInBytes for each row can be expensive; use getRetainedSizeInBytes for estimation
        hiveWriterStats.addInputPageSizesInBytes(dataPage.getRetainedSizeInBytes());
        fileWriter.appendRows(dataPage);
        rowCount += dataPage.getPositionCount();
        inputSizeInBytes += dataPage.getSizeInBytes();
    }

    public void commit()
    {
        List<ColumnStatistics> stats = fileWriter.commit();

        int ordinal = 0;
        List<FileColumnStatistics> statsList = new ArrayList<>();
        for (ColumnStatistics statistics : stats) {
            statsList.add(new FileColumnStatistics(ordinal, statistics.getMin(), statistics.getMax(), statistics.getNumberOfValues()));
            ordinal++;
        }
        FileStatistics fileStatistics = new FileStatistics(statsList);
        fileStats = FILE_COLUMN_STATISTICS_JSON_CODEC.toJson(fileStatistics);

        onCommit.accept(this);
    }

    public class FileColumnStatistics
    {
        private final int ordinal;
        private final String min;
        private final String max;
        private final Long rows;

        @JsonCreator
        public FileColumnStatistics(
                @JsonProperty("ordinal") int ordinal,
                @JsonProperty("min") String min,
                @JsonProperty("max") String max,
                @JsonProperty("rows") Long rows)
        {
            this.ordinal = ordinal;
            this.min = min;
            this.max = max;
            this.rows = rows;
        }

        @JsonProperty
        public int getOrdinal()
        {
            return ordinal;
        }

        @JsonProperty
        public String getMin()
        {
            return min;
        }

        @JsonProperty
        public String getMax()
        {
            return max;
        }

        @JsonProperty
        public Long getRows()
        {
            return rows;
        }
    }

    public class FileStatistics
    {
        private final List<FileColumnStatistics> columnStatistics;

        @JsonCreator
        public FileStatistics(@JsonProperty("columnStatistics") List<FileColumnStatistics> columnStatistics)
        {
            this.columnStatistics = columnStatistics;
        }

        @JsonProperty
        public List<FileColumnStatistics> getColumnStatistics()
        {
            return columnStatistics;
        }
    }

    long getValidationCpuNanos()
    {
        return fileWriter.getValidationCpuNanos();
    }

    public Optional<Runnable> getVerificationTask()
    {
        return fileWriter.getVerificationTask();
    }

    public void rollback()
    {
        fileWriter.rollback();
    }

    public PartitionUpdate getPartitionUpdate()
    {
        return new PartitionUpdate(
                partitionName.orElse(""),
                updateMode,
                writePath,
                targetPath,
                ImmutableList.of(new FileWriteInfo(fileWriteInfo.getWriteFileName(), fileWriteInfo.getTargetFileName(), fileStats)),
                rowCount,
                inputSizeInBytes,
                fileWriter.getWrittenBytes());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileWriter", fileWriter)
                .add("writeFilePath", writePath + "/" + fileWriteInfo.getWriteFileName())
                .add("targetFilePath", targetPath + "/" + fileWriteInfo.getTargetFileName())
                .toString();
    }
}
