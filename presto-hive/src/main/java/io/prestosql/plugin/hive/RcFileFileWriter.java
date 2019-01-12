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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import io.airlift.slice.OutputStreamSliceOutput;
import io.prestosql.rcfile.AircompressorCodecFactory;
import io.prestosql.rcfile.HadoopCodecFactory;
import io.prestosql.rcfile.RcFileDataSource;
import io.prestosql.rcfile.RcFileEncoding;
import io.prestosql.rcfile.RcFileWriter;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static java.util.Objects.requireNonNull;

public class RcFileFileWriter
        implements HiveFileWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RcFileFileWriter.class).instanceSize();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final CountingOutputStream outputStream;
    private final RcFileWriter rcFileWriter;
    private final Callable<Void> rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<RcFileDataSource>> validationInputFactory;

    private long validationCpuNanos;

    public RcFileFileWriter(
            OutputStream outputStream,
            Callable<Void> rollbackAction,
            RcFileEncoding rcFileEncoding,
            List<Type> fileColumnTypes,
            Optional<String> codecName,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            Optional<Supplier<RcFileDataSource>> validationInputFactory)
            throws IOException
    {
        this.outputStream = new CountingOutputStream(outputStream);
        rcFileWriter = new RcFileWriter(
                new OutputStreamSliceOutput(this.outputStream),
                fileColumnTypes,
                rcFileEncoding,
                codecName,
                new AircompressorCodecFactory(new HadoopCodecFactory(getClass().getClassLoader())),
                metadata,
                validationInputFactory.isPresent());
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "outputColumnInputIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
        this.validationInputFactory = validationInputFactory;
    }

    @Override
    public long getWrittenBytes()
    {
        return outputStream.getCount();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + rcFileWriter.getRetainedSizeInBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                blocks[i] = new RunLengthEncodedBlock(nullBlocks.get(i), dataPage.getPositionCount());
            }
            else {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
        }
        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            rcFileWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public void commit()
    {
        try {
            rcFileWriter.close();
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

        if (validationInputFactory.isPresent()) {
            try {
                try (RcFileDataSource input = validationInputFactory.get().get()) {
                    long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                    rcFileWriter.validate(input);
                    validationCpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
                }
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(HIVE_WRITE_VALIDATION_FAILED, e);
            }
        }
    }

    @Override
    public void rollback()
    {
        try {
            try {
                rcFileWriter.close();
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
        return validationCpuNanos;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", rcFileWriter)
                .toString();
    }
}
