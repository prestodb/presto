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
package com.facebook.presto.spiller;

import com.facebook.presto.block.PagesSerde;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.RuntimeIOException;
import io.airlift.slice.SliceOutput;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@NotThreadSafe
public class BinaryFileSpiller
        implements Spiller
{
    private final Path targetDirectory;
    private final Closer closer = Closer.create();
    private final BlockEncodingSerde blockEncodingSerde;
    private final AtomicLong spilledDataSize;

    private final ListeningExecutorService executor;

    private int spillsCount;
    private CompletableFuture<?> previousSpill = CompletableFuture.completedFuture(null);

    public BinaryFileSpiller(
            BlockEncodingSerde blockEncodingSerde,
            ListeningExecutorService executor,
            Path spillPath,
            AtomicLong spilledDataSize)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spilledDataSize = requireNonNull(spilledDataSize, "spilledDataSize is null");
        try {
            this.targetDirectory = Files.createTempDirectory(spillPath, "presto-spill");
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill directory", e);
        }
    }

    @Override
    public CompletableFuture<?> spill(Iterator<Page> pageIterator)
    {
        checkState(previousSpill.isDone());
        Path spillPath = getPath(spillsCount++);

        previousSpill = MoreFutures.toCompletableFuture(executor.submit(
                () -> writePages(pageIterator, spillPath)));
        return previousSpill;
    }

    private void writePages(Iterator<Page> pageIterator, Path spillPath)
    {
        try (SliceOutput output = new OutputStreamSliceOutput(new BufferedOutputStream(new FileOutputStream(spillPath.toFile())))) {
            spilledDataSize.addAndGet(PagesSerde.writePages(blockEncodingSerde, output, pageIterator));
        }
        catch (RuntimeIOException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
    }

    @Override
    public List<Iterator<Page>> getSpills()
    {
        checkState(previousSpill.isDone());
        return IntStream.range(0, spillsCount)
                .mapToObj(i -> readPages(getPath(i)))
                .collect(toImmutableList());
    }

    private Iterator<Page> readPages(Path spillPath)
    {
        try {
            InputStream input = new BufferedInputStream(new FileInputStream(spillPath.toFile()));
            closer.register(input);
            return PagesSerde.readPages(blockEncodingSerde, new InputStreamSliceInput(input));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public void close()
    {
        try (Stream<Path> list = Files.list(targetDirectory)) {
            closer.close();
            for (Path path : list.collect(toList())) {
                Files.delete(path);
            }
            Files.delete(targetDirectory);
        }
        catch (IOException e) {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    String.format("Failed to delete directory [%s]", targetDirectory),
                    e);
        }
    }

    private Path getPath(int spillNumber)
    {
        return Paths.get(targetDirectory.toAbsolutePath().toString(), String.format("%d.bin", spillNumber));
    }
}
