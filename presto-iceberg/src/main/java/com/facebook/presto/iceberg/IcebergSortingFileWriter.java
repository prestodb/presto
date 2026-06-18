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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.SortingFileWriter;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergSortingFileWriter
        implements IcebergFileWriter
{
    private final IcebergFileWriter outputWriter;
    private final SortingFileWriter sortingFileWriter;

    public IcebergSortingFileWriter(
            FileSystem fileSystem,
            Path tempFilePrefix,
            IcebergFileWriter outputWriter,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            boolean sortedWriteToTempPathEnabled,
            ConnectorSession session,
            SortParameters sortParameters)
    {
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");
        this.sortingFileWriter = new SortingFileWriter(
                fileSystem,
                tempFilePrefix,
                outputWriter,
                sortParameters.getSortingFileWriterConfig().getWriterSortBufferSize(),
                sortParameters.getSortingFileWriterConfig().getMaxOpenSortFiles(),
                types,
                sortFields,
                sortOrders,
                sortParameters.getPageSorter(),
                (fs, p) -> sortParameters.getOrcFileWriterFactory().createDataSink(session, fs, p),
                sortedWriteToTempPathEnabled);
    }

    @Override
    public Metrics getMetrics()
    {
        return outputWriter.getMetrics();
    }

    @Override
    public long getWrittenBytes()
    {
        return sortingFileWriter.getWrittenBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return sortingFileWriter.getSystemMemoryUsage();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        sortingFileWriter.appendRows(dataPage);
    }

    @Override
    public Optional<Page> commit()
    {
        return sortingFileWriter.commit();
    }

    @Override
    public void rollback()
    {
        sortingFileWriter.rollback();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return sortingFileWriter.getValidationCpuNanos();
    }

    @Override
    public long getFileSizeInBytes()
    {
        return sortingFileWriter.getFileSizeInBytes();
    }
}
