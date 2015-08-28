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

import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.orc.stream.UnboundedIntegerStream;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.orc.Vector.MAX_VECTOR_LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.OrcReaderUtils.castOrcVector;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DecimalStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;
    private final DecimalType decimalType;
    private final boolean[] isNullVector = new boolean[MAX_VECTOR_LENGTH];
    private final long[] shortUnboundedIntegerVector = new long[MAX_VECTOR_LENGTH];
    private final BigInteger[] longUnboundedIntegerVector = new BigInteger[MAX_VECTOR_LENGTH];
    private final long[] scaleVector = new long[MAX_VECTOR_LENGTH];

    private int readOffset;
    private int nextBatchSize;

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

    public DecimalStreamReader(StreamDescriptor streamDescriptor, DecimalType decimalType)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.decimalType = checkNotNull(decimalType, "decimalType is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        seekToOffset();
        boolean[] isNull = getIsNullVector(vector);
        readStreamsData(isNull);
        fillDecimalsVector(vector, isNull);

        readOffset = 0;
        nextBatchSize = 0;
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

    private boolean[] getIsNullVector(Object vector)
            throws OrcCorruptionException
    {
        if (decimalType.isShort()) {
            return castOrcVector(vector, LongVector.class).isNull;
        }
        else {
            return isNullVector;
        }
    }

    private void readStreamsData(boolean[] isNull)
            throws IOException
    {
        if (presentStream == null) {
            if (unboundedIntegerStream == null) {
                throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
            }
            if (scaleStream == null) {
                throw new OrcCorruptionException("Value is not null but scale stream is not present");
            }

            Arrays.fill(isNull, false);

            if (decimalType.isShort()) {
                unboundedIntegerStream.nextLongVector(nextBatchSize, shortUnboundedIntegerVector);
            }
            else {
                unboundedIntegerStream.nextBigIntegerVector(nextBatchSize, longUnboundedIntegerVector);
            }

            scaleStream.nextLongVector(nextBatchSize, scaleVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, isNull);
            if (nullValues != nextBatchSize) {
                if (unboundedIntegerStream == null) {
                    throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
                }
                if (scaleStream == null) {
                    throw new OrcCorruptionException("Value is not null but scale stream is not present");
                }

                if (decimalType.isShort()) {
                    unboundedIntegerStream.nextLongVector(nextBatchSize, shortUnboundedIntegerVector, isNull);
                }
                else {
                    unboundedIntegerStream.nextBigIntegerVector(nextBatchSize, longUnboundedIntegerVector, isNull);
                }

                scaleStream.nextLongVector(nextBatchSize, scaleVector, isNull);
            }
        }
    }

    private void fillDecimalsVector(Object vector, boolean[] isNull)
            throws OrcCorruptionException
    {
        if (decimalType.isShort()) {
            LongVector shortDecimalVector = castOrcVector(vector, LongVector.class);
            for (int i = 0; i < nextBatchSize; i++) {
                if (!isNull[i]) {
                    long rescaledUnboundedInteger = rescale(shortUnboundedIntegerVector[i], (int) scaleVector[i], decimalType.getScale());
                    shortDecimalVector.vector[i] = rescaledUnboundedInteger;
                }
            }
        }
        else {
            SliceVector longDecimalVector = castOrcVector(vector, SliceVector.class);
            for (int i = 0; i < nextBatchSize; i++) {
                if (!isNull[i]) {
                    BigInteger rescaledUnboundedInteger = rescale(longUnboundedIntegerVector[i], (int) scaleVector[i], decimalType.getScale());
                    longDecimalVector.vector[i] = LongDecimalType.unscaledValueToSlice(rescaledUnboundedInteger);
                }
                else {
                    longDecimalVector.vector[i] = null;
                }
            }
        }
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
