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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfWriterEncryption;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.WriterStats;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static com.facebook.presto.hive.HiveManifestUtils.createFileStatisticsPage;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OrcFileWriter
        implements HiveFileWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcFileWriter.class).instanceSize();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final OrcWriter orcWriter;
    private final Callable<Void> rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<OrcDataSource>> validationInputFactory;

    private long validationCpuNanos;
    private long rowCount;

    public OrcFileWriter(
            DataSink dataSink,
            Callable<Void> rollbackAction,
            OrcEncoding orcEncoding,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            DateTimeZone hiveStorageTimeZone,
            Optional<Supplier<OrcDataSource>> validationInputFactory,
            OrcWriteValidationMode validationMode,
            WriterStats stats,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            Optional<DwrfWriterEncryption> dwrfWriterEncryption)
    {
        requireNonNull(dataSink, "dataSink is null");

        try {
            orcWriter = new OrcWriter(
                    dataSink,
                    columnNames,
                    fileColumnTypes,
                    orcEncoding,
                    compression,
                    dwrfWriterEncryption,
                    dwrfEncryptionProvider,
                    options,
                    metadata,
                    hiveStorageTimeZone,
                    validationInputFactory.isPresent(),
                    validationMode,
                    stats);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }

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
        return orcWriter.getWrittenBytes() + orcWriter.getBufferedBytes();
    }

    @Override
    public long getFileSizeInBytes()
    {
        return orcWriter.getWrittenBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + orcWriter.getRetainedBytes();
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
            orcWriter.write(page);
            rowCount += page.getPositionCount();
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public Optional<Page> commit()
    {
        try {
            orcWriter.close();
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
                try (OrcDataSource input = validationInputFactory.get().get()) {
                    long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                    orcWriter.validate(input);
                    validationCpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
                }
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(HIVE_WRITE_VALIDATION_FAILED, e);
            }
        }

        return Optional.of(createFileStatisticsPage(getFileSizeInBytes(), rowCount));
    }

    @Override
    public void rollback()
    {
        try {
            try {
                orcWriter.close();
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
                .add("writer", orcWriter)
                .toString();
    }
}
