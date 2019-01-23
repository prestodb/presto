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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LongDirectStreamReader
        extends ColumnReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDirectStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;
    private boolean[] nullVector = new boolean[0];

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private LocalMemoryContext systemMemoryContext;

    private long[] values;
    private boolean[] valueIsNull;

    public LongDirectStreamReader(StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                dataStream.skip(readOffset);
            }
        }

        BlockBuilder builder = type.createBlockBuilder(null, nextBatchSize);
        if (presentStream == null) {
            if (dataStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            dataStream.nextLongVector(type, nextBatchSize, builder);
        }
        else {
            for (int i = 0; i < nextBatchSize; i++) {
                if (presentStream.nextBit()) {
                    verify(dataStream != null);
                    type.writeLong(builder, dataStream.next());
                }
                else {
                    builder.appendNull();
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return builder.build();
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void erase(int end)
    {
        if (values == null) {
            return;
        }
        numValues -= end;
        if (numValues > 0) {
            System.arraycopy(values, end, values, 0, numValues);
            if (valueIsNull != null) {
                System.arraycopy(valueIsNull, end, valueIsNull, 0, numValues);
            }
        }
    }

    public void compactValues(int[] positions, int base, int numPositions)
    {
        if (outputChannel != -1) {
            StreamReaders.compactArrays(positions, base, numPositions, values, valueIsNull);
            numValues = base + numPositions;
        }
        compactQualifyingSet(positions, numPositions);
    }

    @Override
    public int getFixedWidth()
    {
        return SIZE_OF_LONG;
    }

    @Override
    public void scan()
            throws IOException
    {
        beginScan(presentStream, null);
        if (presentStream != null) {
            throw new UnsupportedOperationException("scan() does not support nulls");
        }
        if (outputChannel != -1) {
            ensureValuesSize();
        }
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        int numInput = input.getPositionCount();
        if (filter != null) {
            if (outputQualifyingSet == null) {
                outputQualifyingSet = new QualifyingSet();
                output = outputQualifyingSet;
            }
            numResults = dataStream.scan(filter,
                                     input.getPositions(),
                                     numInput,
                                     input.getEnd(),
                                     input.getPositions(),
                                     null,
                                     output.getMutablePositions(numInput),
                                     output.getMutableInputNumbers(numInput),
                                     values,
                                     numValues);
            output.setEnd(input.getEnd());
        }
        else {
            numResults = dataStream.scan(null,
                                     input.getPositions(),
                                     numInput,
                                     input.getEnd(),
                                     null,
                                     null,
                                     null,
                                     null,
                                     values,
                                     numValues);
        }
        endScan(presentStream);
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        if (mayReuse) {
            return new LongArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
        }
        if (numFirstRows < numValues || values.length > (int) (numFirstRows * 1.2)) {
            return new LongArrayBlock(numFirstRows,
                                      valueIsNull == null ? Optional.empty() : Optional.of(Arrays.copyOf(valueIsNull, numFirstRows)),
                                      Arrays.copyOf(values, numFirstRows));
        }
        Block block = new LongArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
        values = null;
        valueIsNull = null;
        numValues = 0;
        return block;
    }

    private void ensureValuesSize()
    {
        if (outputChannel == -1) {
            return;
        }
        int numInput = inputQualifyingSet.getPositionCount();
        if (values == null) {
            values = new long[Math.max(numInput, expectNumValues)];
        }
        else if (numValues + numInput > values.length) {
            int newSize = (int) ((numValues + numInput) * 1.2);
            if (valueIsNull != null) {
                valueIsNull = Arrays.copyOf(valueIsNull, newSize);
            }
            values = Arrays.copyOf(values, newSize);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
        nullVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector);
    }
}
