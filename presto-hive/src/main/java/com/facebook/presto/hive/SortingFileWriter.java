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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.hive.util.MergingPageIterator;
import com.facebook.presto.hive.util.SortBuffer;
import com.facebook.presto.hive.util.TempFileReader;
import com.facebook.presto.hive.util.TempFileWriter;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class SortingFileWriter
        implements HiveFileWriter
{
    private static final Logger log = Logger.get(SortingFileWriter.class);

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SortingFileWriter.class).instanceSize();

    private final FileSystem fileSystem;
    private final Path tempFilePrefix;
    private final int maxOpenTempFiles;
    private final List<Type> types;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final HiveFileWriter outputWriter;
    private final SortBuffer sortBuffer;
    private final TempFileSinkFactory tempFileSinkFactory;
    private final boolean sortedWriteToTempPathEnabled;
    private final Queue<TempFile> tempFiles = new PriorityQueue<>(comparing(TempFile::getSize));

    public SortingFileWriter(
            FileSystem fileSystem,
            Path tempFilePrefix,
            HiveFileWriter outputWriter,
            DataSize maxMemory,
            int maxOpenTempFiles,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            PageSorter pageSorter,
            TempFileSinkFactory tempFileSinkFactory,
            boolean sortedWriteToTempPathEnabled)
    {
        checkArgument(maxOpenTempFiles >= 2, "maxOpenTempFiles must be at least two");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.tempFilePrefix = requireNonNull(tempFilePrefix, "tempFilePrefix is null");
        this.maxOpenTempFiles = maxOpenTempFiles;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortFields = ImmutableList.copyOf(requireNonNull(sortFields, "sortFields is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");
        this.sortBuffer = new SortBuffer(maxMemory, types, sortFields, sortOrders, pageSorter);
        this.tempFileSinkFactory = tempFileSinkFactory;
        this.sortedWriteToTempPathEnabled = sortedWriteToTempPathEnabled;
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
    public Optional<Page> commit()
    {
        if (!sortBuffer.isEmpty()) {
            // skip temporary files entirely if the total output size is small
            if (tempFiles.isEmpty()) {
                sortBuffer.flushTo(outputWriter::appendRows);
                return outputWriter.commit();
            }

            flushToTempFile();
        }

        try {
            writeSorted();
            return outputWriter.commit();
        }
        catch (UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
    }

    @Override
    public void rollback()
    {
        if (!sortedWriteToTempPathEnabled) {
            for (TempFile file : tempFiles) {
                cleanupFile(file.getPath());
            }
        }

        outputWriter.rollback();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return outputWriter.getValidationCpuNanos();
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

    @Override
    public long getFileSizeInBytes()
    {
        return getWrittenBytes();
    }

    private void flushToTempFile()
    {
        writeTempFile(writer -> sortBuffer.flushTo(writer::writePage));
    }

    // TODO: change connector SPI to make this resumable and have memory tracking
    private void writeSorted()
    {
        combineFiles();

        mergeFiles(tempFiles, outputWriter::appendRows);
    }

    private void combineFiles()
    {
        while (tempFiles.size() > maxOpenTempFiles) {
            int count = min(maxOpenTempFiles, tempFiles.size() - (maxOpenTempFiles - 1));

            List<TempFile> smallestFiles = IntStream.range(0, count)
                    .mapToObj(i -> tempFiles.poll())
                    .collect(toImmutableList());

            writeTempFile(writer -> mergeFiles(smallestFiles, writer::writePage));
        }
    }

    private void mergeFiles(Iterable<TempFile> files, Consumer<Page> consumer)
    {
        try (Closer closer = Closer.create()) {
            Collection<Iterator<Page>> iterators = new ArrayList<>();

            for (TempFile tempFile : files) {
                Path file = tempFile.getPath();
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
                    .forEachRemaining(consumer);

            if (!sortedWriteToTempPathEnabled) {
                for (TempFile tempFile : files) {
                    Path file = tempFile.getPath();
                    fileSystem.delete(file, false);
                    if (fileSystem.exists(file)) {
                        throw new IOException("Failed to delete temporary file: " + file);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeTempFile(Consumer<TempFileWriter> consumer)
    {
        Path tempFile = getTempFileName();

        try (TempFileWriter writer = new TempFileWriter(types, tempFileSinkFactory.createSink(fileSystem, tempFile))) {
            consumer.accept(writer);
            writer.close();
            tempFiles.add(new TempFile(tempFile, writer.getWrittenBytes()));
        }
        catch (IOException | UncheckedIOException e) {
            if (!sortedWriteToTempPathEnabled) {
                cleanupFile(tempFile);
            }
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, "Failed to write temporary file: " + tempFile, e);
        }
    }

    private void cleanupFile(Path file)
    {
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

    private Path getTempFileName()
    {
        return new Path(tempFilePrefix + "." + randomUUID().toString().replaceAll("-", "_"));
    }

    private static class TempFile
    {
        private final Path path;
        private final long size;

        public TempFile(Path path, long size)
        {
            checkArgument(size >= 0, "size is negative");
            this.path = requireNonNull(path, "path is null");
            this.size = size;
        }

        public Path getPath()
        {
            return path;
        }

        public long getSize()
        {
            return size;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("path", path)
                    .add("size", size)
                    .toString();
        }
    }

    public interface TempFileSinkFactory
    {
        DataSink createSink(FileSystem fileSystem, Path path)
                throws IOException;
    }
}
