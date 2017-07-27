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

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_DICTIONARY_MEMORY_MAX_SIZE;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_ROW_GROUP_MAX_ROW_COUNT;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_STRIPE_MAX_ROW_COUNT;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_STRIPE_MAX_SIZE;
import static com.facebook.presto.orc.OrcWriter.DEFAULT_STRIPE_MIN_ROW_COUNT;
import static com.facebook.presto.orc.OrcWriter.createDwrfWriter;
import static com.facebook.presto.orc.OrcWriter.createOrcWriter;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OrcFileWriter
        implements HiveFileWriter
{
    private final OrcWriter orcWriter;
    private final Callable<Void> rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<OrcDataSource>> validationInputFactory;

    public OrcFileWriter(
            OutputStream outputStream,
            Callable<Void> rollbackAction,
            boolean isDwrf,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            CompressionKind compression,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            DateTimeZone hiveStorageTimeZone,
            Optional<Supplier<OrcDataSource>> validationInputFactory)
    {
        if (!(outputStream instanceof SliceOutput)) {
            outputStream = new OutputStreamSliceOutput(outputStream);
        }

        if (isDwrf) {
            orcWriter = createDwrfWriter(
                    (SliceOutput) outputStream,
                    columnNames,
                    fileColumnTypes,
                    compression,
                    DEFAULT_STRIPE_MAX_SIZE,
                    DEFAULT_STRIPE_MIN_ROW_COUNT,
                    DEFAULT_STRIPE_MAX_ROW_COUNT,
                    DEFAULT_ROW_GROUP_MAX_ROW_COUNT,
                    DEFAULT_DICTIONARY_MEMORY_MAX_SIZE,
                    metadata,
                    hiveStorageTimeZone,
                    validationInputFactory.isPresent());
        }
        else {
            orcWriter = createOrcWriter(
                    (SliceOutput) outputStream,
                    columnNames,
                    fileColumnTypes,
                    compression,
                    DEFAULT_STRIPE_MAX_SIZE,
                    DEFAULT_STRIPE_MIN_ROW_COUNT,
                    DEFAULT_STRIPE_MAX_ROW_COUNT,
                    DEFAULT_ROW_GROUP_MAX_ROW_COUNT,
                    DEFAULT_DICTIONARY_MEMORY_MAX_SIZE,
                    metadata,
                    hiveStorageTimeZone,
                    validationInputFactory.isPresent());
        }
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "outputColumnInputIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(new BlockBuilderStatus(), 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
        this.validationInputFactory = validationInputFactory;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return orcWriter.getRetainedBytes();
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
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public void commit()
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
                    orcWriter.validate(input);
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
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", orcWriter)
                .toString();
    }
}
