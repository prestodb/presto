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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.facebook.presto.common.Page;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.reader.ParquetSelectiveReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParquetSelectivePageSource
        implements ConnectorPageSource
{
    private final ParquetSelectiveReader parquetSelectiveReader;
    private final ParquetDataSource parquetDataSource;
    private final FileFormatDataSourceStats stats;
    private final AggregatedMemoryContext systemMemoryContext;

    private boolean closed;

    public ParquetSelectivePageSource(
            ParquetSelectiveReader parquetSelectiveReader,
            ParquetDataSource parquetDataSource,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats)
    {
        this.parquetSelectiveReader = requireNonNull(parquetSelectiveReader, "parquetSelectiveReader is null");
        this.parquetDataSource = requireNonNull(parquetDataSource, "orcDataSource is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetDataSource.getReadBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return parquetSelectiveReader.getReadPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = parquetSelectiveReader.getNextPage();
            if (page == null) {
                close();
            }

            return page;
        }
        catch (InvalidFunctionArgumentException e) {
            closeWithSuppression(e);
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (UnsupportedOperationException e) {
            closeWithSuppression(e);
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (OrcCorruptionException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read Parquet file: %s", parquetDataSource.getId()), e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetSelectiveReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0; //systemMemoryContext.getBytes();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }
}
