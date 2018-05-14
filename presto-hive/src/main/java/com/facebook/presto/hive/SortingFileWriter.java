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

import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.hive.util.MergingPageIterator;
import com.facebook.presto.hive.util.SortBuffer;
import com.facebook.presto.hive.util.TempFileReader;
import com.facebook.presto.hive.util.TempFileWriter;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_BUCKET_SORT_FILES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class SortingFileWriter
        implements HiveFileWriter
{
    private static final Logger log = Logger.get(SortingFileWriter.class);

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SortingFileWriter.class).instanceSize();

    private final FileSystem fileSystem;
    private final Path tempFilePrefix;
    private final int maxTempFiles;
    private final List<Type> types;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final HiveFileWriter outputWriter;
    private final SortBuffer sortBuffer;
    private final List<Path> tempFiles = new ArrayList<>();

    public SortingFileWriter(
            FileSystem fileSystem,
            Path tempFilePrefix,
            HiveFileWriter outputWriter,
            DataSize maxMemory,
            int maxTempFiles,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            PageSorter pageSorter)
    {
        checkArgument(maxTempFiles > 0, "maxTempFiles must be greater than zero");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.tempFilePrefix = requireNonNull(tempFilePrefix, "tempFilePrefix is null");
        this.maxTempFiles = maxTempFiles;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortFields = ImmutableList.copyOf(requireNonNull(sortFields, "sortFields is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");
        this.sortBuffer = new SortBuffer(maxMemory, types, sortFields, sortOrders, pageSorter);
    }

    @Override
    public long getWrittenBytes()
    {
        return outputWriter.getWrittenBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + sortBuffer.getRetainedBytes();
    }

    @Override
    public void appendRows(Page page)
    {
        if (!sortBuffer.canAdd(page)) {
            flushToTempFile();
        }
        sortBuffer.add(page);
    }

    @Override
    public void commit()
    {
        if (!sortBuffer.isEmpty()) {
            // skip temporary files entirely if the total output size is small
            if (tempFiles.isEmpty()) {
                sortBuffer.flushTo(outputWriter::appendRows);
                outputWriter.commit();
                return;
            }

            flushToTempFile();
        }

        try {
            writeSorted();
            outputWriter.commit();

            for (Path file : tempFiles) {
                fileSystem.delete(file, false);
                if (fileSystem.exists(file)) {
                    throw new IOException("Failed to delete temporary file: " + file);
                }
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
    }

    @Override
    public void rollback()
    {
        for (Path file : tempFiles) {
            try {
                fileSystem.delete(file, false);
                if (fileSystem.exists(file)) {
                    throw new IOException("Delete failed");
                }
            }
            catch (IOException e) {
                log.warn(e, "Failed to delete temporary file: " + file);
            }
        }

        outputWriter.rollback();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tempFilePrefix", tempFilePrefix)
                .add("outputWriter", outputWriter)
                .toString();
    }

    @Override
    public Optional<Runnable> getVerificationTask()
    {
        return outputWriter.getVerificationTask();
    }

    // TODO: change connector SPI to make this resumable and have memory tracking
    private void writeSorted()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            Collection<Iterator<Page>> iterators = new ArrayList<>();

            for (Path file : tempFiles) {
                OrcDataSource dataSource = new HdfsOrcDataSource(
                        new OrcDataSourceId(file.toString()),
                        fileSystem.getFileStatus(file).getLen(),
                        new DataSize(1, MEGABYTE),
                        new DataSize(8, MEGABYTE),
                        new DataSize(8, MEGABYTE),
                        false,
                        fileSystem.open(file),
                        new FileFormatDataSourceStats());
                closer.register(dataSource);
                iterators.add(new TempFileReader(types, dataSource));
            }

            new MergingPageIterator(iterators, types, sortFields, sortOrders)
                    .forEachRemaining(outputWriter::appendRows);
        }
    }

    private void flushToTempFile()
    {
        if (tempFiles.size() == maxTempFiles) {
            throw new PrestoException(HIVE_TOO_MANY_BUCKET_SORT_FILES, "Too many temporary files for sorted bucket writer");
        }

        Path tempFile = new Path(tempFilePrefix + "." + tempFiles.size());
        tempFiles.add(tempFile);

        try (TempFileWriter writer = new TempFileWriter(types, fileSystem.create(tempFile))) {
            sortBuffer.flushTo(writer::writePage);
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, "Failed to write temporary file: " + tempFile, e);
        }
    }
}
