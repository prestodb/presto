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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.parquet.AbstractParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsParquetDataSource
        extends AbstractParquetDataSource
{
    private final FSDataInputStream inputStream;
    private final FileFormatDataSourceStats stats;

    public HdfsParquetDataSource(ParquetDataSourceId id, FSDataInputStream inputStream, FileFormatDataSourceStats stats)
    {
        super(id);
        this.stats = requireNonNull(stats, "stats is null");
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            long start = System.nanoTime();
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
            stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - start);
        }
        catch (PrestoException e) {
            // just in case there is a Presto wrapper or hook
            throw e;
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Error reading from %s at position %s", getId(), position), e);
        }
    }

    public static HdfsParquetDataSource buildHdfsParquetDataSource(FileSystem fileSystem, Path path, long start, long length, FileFormatDataSourceStats stats)
    {
        try {
            FSDataInputStream inputStream = fileSystem.open(path);
            return new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), inputStream, stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage()), e);
        }
    }

    public static HdfsParquetDataSource buildHdfsParquetDataSource(FSDataInputStream inputStream, Path path, FileFormatDataSourceStats stats)
    {
        return new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), inputStream, stats);
    }
}
