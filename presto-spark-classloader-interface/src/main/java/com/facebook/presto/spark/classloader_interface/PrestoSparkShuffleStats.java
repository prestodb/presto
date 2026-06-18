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
package com.facebook.presto.spark.classloader_interface;

import java.io.Serializable;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoSparkShuffleStats
        implements Serializable
{
    private final int fragmentId;
    private final int taskId;
    private final Operation operation;
    private final long processedRows;
    private final long processedRowBatches;
    private final long processedBytes;
    private final long elapsedWallTimeMills;

    public PrestoSparkShuffleStats(
            int fragmentId,
            int taskId,
            Operation operation,
            long processedRows,
            long processedRowBatches,
            long processedBytes,
            long elapsedWallTimeMills)
    {
        this.fragmentId = fragmentId;
        this.taskId = taskId;
        this.operation = requireNonNull(operation, "operation is null");
        checkArgument(processedRows >= 0, "processedRows must be greater than or equal to zero: %s", processedRows);
        this.processedRows = processedRows;
        checkArgument(processedRowBatches >= 0, "processedRowBatches must be greater than or equal to zero: %s", processedRowBatches);
        this.processedRowBatches = processedRowBatches;
        checkArgument(processedBytes >= 0, "processedBytes must be greater than or equal to zero: %s", processedBytes);
        this.processedBytes = processedBytes;
        checkArgument(elapsedWallTimeMills >= 0, "elapsedWallTimeMills must be greater than or equal to zero: %s", elapsedWallTimeMills);
        this.elapsedWallTimeMills = elapsedWallTimeMills;
    }

    public int getFragmentId()
    {
        return fragmentId;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public long getProcessedRows()
    {
        return processedRows;
    }

    public long getProcessedRowBatches()
    {
        return processedRowBatches;
    }

    public long getProcessedBytes()
    {
        return processedBytes;
    }

    public long getElapsedWallTimeMills()
    {
        return elapsedWallTimeMills;
    }

    public enum Operation
    {
        READ,
        WRITE
    }

    @Override
    public String toString()
    {
        return "";
    }

    private static void checkArgument(boolean condition, String message, Object... args)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, args));
        }
    }
}
