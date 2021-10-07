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
package com.facebook.presto.delta;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_READ_DATA_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * {@link ConnectorPageSource} implementation for Delta tables that prefills
 * partition column blocks and combines them with regular column blocks returned
 * by the underlying file reader {@link ConnectorPageSource} implementation.
 */
public class DeltaPageSource
        implements ConnectorPageSource
{
    private final List<DeltaColumnHandle> columnHandles;
    private final ConnectorPageSource dataPageSource;
    private final Map<String, Block> partitionValues;

    /**
     * Create a DeltaPageSource
     *
     * @param columnHandles   List of columns (includes partition and regular) in order for which data needed in output.
     * @param partitionValues Partition values (partition column -> partition value map).
     * @param dataPageSource  Initialized underlying file reader which returns the data for regular columns.
     */
    public DeltaPageSource(
            List<DeltaColumnHandle> columnHandles,
            Map<String, Block> partitionValues,
            ConnectorPageSource dataPageSource)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = dataPageSource.getNextPage();
            if (dataPage == null) {
                return null; // reader is done
            }
            int positionCount = dataPage.getPositionCount();

            int dataColumnIndex = 0;
            int columnIndex = 0;
            Block[] blocksWithPartitionColumns = new Block[columnHandles.size()];
            for (DeltaColumnHandle columnHandle : columnHandles) {
                if (columnHandle.getColumnType() == PARTITION) {
                    Block partitionValue = partitionValues.get(columnHandle.getName());
                    blocksWithPartitionColumns[columnIndex++] = new RunLengthEncodedBlock(partitionValue, positionCount);
                }
                else {
                    blocksWithPartitionColumns[columnIndex++] = dataPage.getBlock(dataColumnIndex);
                    dataColumnIndex++;
                }
            }
            return new Page(positionCount, blocksWithPartitionColumns);
        }
        catch (PrestoException exception) {
            closeWithSuppression(exception);
            throw exception; // already properly handled exception - throw without any additional info
        }
        catch (RuntimeException exception) {
            closeWithSuppression(exception);
            throw new PrestoException(DELTA_READ_DATA_ERROR, exception);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (Exception exception) {
            // Self-suppression not permitted
            if (exception != throwable) {
                throwable.addSuppressed(exception);
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return dataPageSource.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return dataPageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return dataPageSource.isFinished();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return dataPageSource.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        dataPageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return dataPageSource.isBlocked();
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return dataPageSource.getRuntimeStats();
    }
}
