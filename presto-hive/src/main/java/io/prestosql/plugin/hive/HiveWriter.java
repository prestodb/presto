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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.PartitionUpdate.UpdateMode;
import io.prestosql.spi.Page;

import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveWriter
{
    private final HiveFileWriter fileWriter;
    private final Optional<String> partitionName;
    private final UpdateMode updateMode;
    private final String fileName;
    private final String writePath;
    private final String targetPath;
    private final Consumer<HiveWriter> onCommit;
    private final HiveWriterStats hiveWriterStats;

    private long rowCount;
    private long inputSizeInBytes;

    public HiveWriter(
            HiveFileWriter fileWriter,
            Optional<String> partitionName,
            UpdateMode updateMode,
            String fileName,
            String writePath,
            String targetPath,
            Consumer<HiveWriter> onCommit,
            HiveWriterStats hiveWriterStats)
    {
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.updateMode = requireNonNull(updateMode, "updateMode is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
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
        fileWriter.commit();
        onCommit.accept(this);
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
                ImmutableList.of(fileName),
                rowCount,
                inputSizeInBytes,
                fileWriter.getWrittenBytes());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileWriter", fileWriter)
                .add("filePath", writePath + "/" + fileName)
                .toString();
    }
}
