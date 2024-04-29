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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.CompressionKind;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcWrite
        implements Closeable
{
    private enum State
    {
        INITIALIZING,
        WRITING,
        FAILED,
        CLOSED;
    }

    private final File outputFile;
    private OrcWriterOptions writerOptions = OrcWriterOptions.getDefaultOrcWriterOptions();
    private List<Type> types;
    private List<String> columnNames;
    private CompressionKind compressionKind = CompressionKind.ZLIB;
    private OrcEncoding encoding = OrcEncoding.DWRF;
    private State state = State.INITIALIZING;
    private OrcWriter orcWriter;

    public OrcWrite(File outputFile)
    {
        this.outputFile = requireNonNull(outputFile, "outputFile is null");
    }

    public OrcWrite withTypes(Type... types)
    {
        return withTypes(Arrays.stream(types).collect(toList()));
    }

    public OrcWrite withTypes(List<Type> types)
    {
        checkIsInitializing();
        this.types = requireNonNull(types, "types is null");
        return this;
    }

    public OrcWrite withColumnNames(List<String> columnNames)
    {
        checkIsInitializing();
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
        return this;
    }

    public OrcWrite withWriterOptions(OrcWriterOptions writerOptions)
    {
        checkIsInitializing();
        this.writerOptions = requireNonNull(writerOptions, "writerOptions is null");
        return this;
    }

    public OrcWrite withWriterOptions(BiConsumer<DefaultOrcWriterFlushPolicy.Builder, OrcWriterOptions.Builder> consumer)
    {
        checkIsInitializing();

        DefaultOrcWriterFlushPolicy.Builder flushPolicyBuilder = DefaultOrcWriterFlushPolicy.builder();
        OrcWriterOptions.Builder writerOptionsBuilder = OrcWriterOptions.builder();
        consumer.accept(flushPolicyBuilder, writerOptionsBuilder);
        writerOptionsBuilder.withFlushPolicy(flushPolicyBuilder.build());
        return withWriterOptions(writerOptionsBuilder.build());
    }

    public OrcWrite withCompressionKind(CompressionKind compressionKind)
    {
        checkIsInitializing();
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        return this;
    }

    public OrcWrite withEncoding(OrcEncoding encoding)
    {
        checkIsInitializing();
        this.encoding = requireNonNull(encoding, "encoding is null");
        return this;
    }

    public OrcWrite writePage(Page page)
    {
        maybeCreateWriter();
        try {
            orcWriter.write(page);
        }
        catch (Exception e) {
            state = State.FAILED;
            forceClose();
            throw new RuntimeException("Failed to write a page", e);
        }
        return this;
    }

    public OrcWrite writePages(List<Page> pages)
    {
        maybeCreateWriter();
        try {
            for (Page page : pages) {
                orcWriter.write(page);
            }
        }
        catch (Exception e) {
            state = State.FAILED;
            forceClose();
            throw new RuntimeException("Failed to write pages", e);
        }
        return this;
    }

    public OrcWrite writePages(Page... pages)
    {
        return writePages(Arrays.stream(pages).collect(toList()));
    }

    public void close()
    {
        if (state == State.INITIALIZING) {
            maybeCreateWriter();
        }
        checkState(state == State.WRITING, "Must be in the WRITING state");
        try {
            orcWriter.close();
            state = State.CLOSED;
        }
        catch (Exception e) {
            state = State.FAILED;
            throw new RuntimeException("Failed to close the writer", e);
        }
    }

    private void forceClose()
    {
        if (orcWriter != null) {
            try {
                orcWriter.close();
            }
            catch (Exception e) {
                // ignore
            }
        }
    }

    private void maybeCreateWriter()
    {
        checkState(state == State.INITIALIZING || state == State.WRITING, "Must be either in the INITIALIZING or WRITING state");

        requireNonNull(types, "types is not set");
        if (columnNames == null) {
            columnNames = makeColumnNames(types.size());
        }

        if (state == State.INITIALIZING) {
            try {
                this.orcWriter = createOrcWriter(
                        outputFile,
                        encoding,
                        compressionKind,
                        Optional.empty(),
                        types,
                        writerOptions,
                        NOOP_WRITER_STATS);
                state = State.WRITING;
            }
            catch (Exception e) {
                state = State.FAILED;
                throw new RuntimeException("Failed to create a writer", e);
            }
        }
    }

    private static List<String> makeColumnNames(int columnCount)
    {
        return IntStream.range(0, columnCount)
                .mapToObj(i -> "test" + i)
                .collect(toList());
    }

    private void checkIsInitializing()
    {
        checkState(state == State.INITIALIZING, "Must be in the INITIALIZING state");
    }
}
