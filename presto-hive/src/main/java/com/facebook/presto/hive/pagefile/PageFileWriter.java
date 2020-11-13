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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HiveFileWriter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PagesSerde;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.Callable;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static java.util.Objects.requireNonNull;

public class PageFileWriter
        implements HiveFileWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PageFileWriter.class).instanceSize();

    private final PageWriter pageWriter;
    private final PagesSerde pagesSerde;
    private final Callable<Void> rollbackAction;

    public PageFileWriter(
            DataSink dataSink,
            PagesSerde pagesSerde,
            HiveCompressionCodec compression,
            DataSize pageFileStripeMaxSize,
            Callable<Void> rollbackAction)
    {
        pageWriter = new PageWriter(dataSink, compression, pageFileStripeMaxSize);
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");
    }

    @Override
    public long getWrittenBytes()
    {
        return pageWriter.getWrittenBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + pageWriter.getRetainedBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        try {
            pageWriter.write(pagesSerde.serialize(dataPage));
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Optional<Page> commit()
    {
        try {
            pageWriter.close();
            return Optional.empty();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.call();
            }
            catch (Exception ignored) {
                // ignore
            }
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
    }

    @Override
    public void rollback()
    {
        try {
            try {
                pageWriter.closeWithoutWrite();
            }
            finally {
                rollbackAction.call();
            }
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public long getFileSizeInBytes()
    {
        return getWrittenBytes();
    }
}
