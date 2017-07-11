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

import com.facebook.presto.rcfile.AircompressorCodecFactory;
import com.facebook.presto.rcfile.HadoopCodecFactory;
import com.facebook.presto.rcfile.RcFileDataSource;
import com.facebook.presto.rcfile.RcFileEncoding;
import com.facebook.presto.rcfile.RcFileWriter;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RcFileFileWriter
        implements HiveFileWriter
{
    private final RcFileWriter rcFileWriter;
    private final Callable<Void> rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final Optional<Supplier<RcFileDataSource>> validationInputFactory;

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
        rcFileWriter = new RcFileWriter(
                outputStream instanceof SliceOutput ? ((SliceOutput) outputStream) : new OutputStreamSliceOutput(outputStream),
                fileColumnTypes,
                rcFileEncoding,
                codecName,
                new AircompressorCodecFactory(new HadoopCodecFactory(getClass().getClassLoader())),
                metadata,
                validationInputFactory.isPresent());
        this.rollbackAction = rollbackAction;

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
        return rcFileWriter.getRetainedSizeInBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        Block[] blocks = new Block[fileInputColumnIndexes.length];
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex >= 0) {
                blocks[i] = dataPage.getBlock(inputColumnIndex);
            }
            else {
                blocks[i] = new RunLengthEncodedBlock(nullBlocks.get(i), dataPage.getPositionCount());
            }
        }
        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            rcFileWriter.write(page);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    @Override
    public void commit()
    {
        try {
            rcFileWriter.close();
        }
        catch (IOException e) {
            try {
                rollbackAction.call();
            }
            catch (Exception e2) {
                // ignore
            }
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }

        if (validationInputFactory.isPresent()) {
            try {
                try (RcFileDataSource input = validationInputFactory.get().get()) {
                    rcFileWriter.validate(input);
                }
            }
            catch (IOException e) {
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
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", rcFileWriter)
                .toString();
    }
}
