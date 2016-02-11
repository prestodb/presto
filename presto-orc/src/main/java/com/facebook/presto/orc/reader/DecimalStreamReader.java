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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.orc.stream.UnboundedIntegerStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DecimalStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private boolean[] nullVector = new boolean[0];
    private long[] shortUnboundedIntegerVector = new long[0];
    private BigInteger[] longUnboundedIntegerVector = new BigInteger[0];
    private long[] scaleVector = new long[0];

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<UnboundedIntegerStream> unboundedIntegerStreamSource = missingStreamSource(UnboundedIntegerStream.class);
    @Nullable
    private UnboundedIntegerStream unboundedIntegerStream;

    @Nonnull
    private StreamSource<LongStream> scaleStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream scaleStream;

    private boolean rowGroupOpen;

    public DecimalStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
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
        DecimalType decimalType = (DecimalType) type;

        if (!rowGroupOpen) {
            openRowGroup();
        }

        seekToOffset();
        allocateVectors();
        readStreamsData(decimalType);
        Block block = buildDecimalsBlock(decimalType);

        readOffset = 0;
        nextBatchSize = 0;

        return block;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        unboundedIntegerStream = unboundedIntegerStreamSource.openStream();
        scaleStream = scaleStreamSource.openStream();
        rowGroupOpen = true;
    }

    private void seekToOffset()
            throws IOException
    {
        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (unboundedIntegerStream == null) {
                    throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
                }
                if (scaleStream == null) {
                    throw new OrcCorruptionException("Value is not null but scale stream is not present");
                }

                unboundedIntegerStream.skip(readOffset);
                scaleStream.skip(readOffset);
            }
        }
    }

    private void allocateVectors()
    {
        if (nullVector.length < nextBatchSize) {
            nullVector = new boolean[nextBatchSize];
            shortUnboundedIntegerVector = new long[nextBatchSize];
            longUnboundedIntegerVector = new BigInteger[nextBatchSize];
            scaleVector = new long[nextBatchSize];
        }
    }

    private void readStreamsData(DecimalType decimalType)
            throws IOException
    {
        if (presentStream == null) {
            if (unboundedIntegerStream == null) {
                throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
            }
            if (scaleStream == null) {
                throw new OrcCorruptionException("Value is not null but scale stream is not present");
            }

            Arrays.fill(nullVector, false);

            if (decimalType.isShort()) {
                unboundedIntegerStream.nextLongVector(nextBatchSize, shortUnboundedIntegerVector);
            }
            else {
                unboundedIntegerStream.nextBigIntegerVector(nextBatchSize, longUnboundedIntegerVector);
            }

            scaleStream.nextLongVector(nextBatchSize, scaleVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (unboundedIntegerStream == null) {
                    throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
                }
                if (scaleStream == null) {
                    throw new OrcCorruptionException("Value is not null but scale stream is not present");
                }

                if (decimalType.isShort()) {
                    unboundedIntegerStream.nextLongVector(nextBatchSize, shortUnboundedIntegerVector, nullVector);
                }
                else {
                    unboundedIntegerStream.nextBigIntegerVector(nextBatchSize, longUnboundedIntegerVector, nullVector);
                }

                scaleStream.nextLongVector(nextBatchSize, scaleVector, nullVector);
            }
        }
    }

    private Block buildDecimalsBlock(DecimalType decimalType)
            throws OrcCorruptionException
    {
        BlockBuilder builder = decimalType.createBlockBuilder(new BlockBuilderStatus(), nextBatchSize);

        if (decimalType.isShort()) {
            for (int i = 0; i < nextBatchSize; i++) {
                if (!nullVector[i]) {
                    long rescaledUnboundedInteger = rescale(shortUnboundedIntegerVector[i], (int) scaleVector[i], decimalType.getScale());
                    decimalType.writeLong(builder, rescaledUnboundedInteger);
                }
                else {
                    builder.appendNull();
                }
            }
        }
        else {
            for (int i = 0; i < nextBatchSize; i++) {
                if (!nullVector[i]) {
                    BigInteger rescaledUnboundedInteger = rescale(longUnboundedIntegerVector[i], (int) scaleVector[i], decimalType.getScale());
                    Slice slice = LongDecimalType.unscaledValueToSlice(rescaledUnboundedInteger);
                    decimalType.writeSlice(builder, slice);
                }
                else {
                    builder.appendNull();
                }
            }
        }

        return builder.build();
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        unboundedIntegerStreamSource = missingStreamSource(UnboundedIntegerStream.class);
        scaleStreamSource = missingStreamSource(LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        unboundedIntegerStream = null;
        scaleStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        unboundedIntegerStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, UnboundedIntegerStream.class);
        scaleStreamSource = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        unboundedIntegerStream = null;
        scaleStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    public static long rescale(long value, int fromScale, int toScale)
    {
        checkState(fromScale <= toScale, "target scale must be larger than source scale");
        return value * ShortDecimalType.tenToNth(toScale - fromScale);
    }

    public static BigInteger rescale(BigInteger value, int fromScale, int toScale)
    {
        checkState(fromScale <= toScale, "target scale must be larger than source scale");
        return value.multiply(LongDecimalType.tenToNth(toScale - fromScale));
    }
}
