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
package com.facebook.presto.orc.block;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.reader.DecimalStreamReader;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.orc.stream.UnboundedIntegerStream;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class DecimalBlockReader
        implements BlockReader
{
    private final StreamDescriptor streamDescriptor;
    private final DecimalType type;

    @Nullable
    private BooleanStream presentStream;
    @Nullable
    private UnboundedIntegerStream unboundedIntegerStream;
    @Nullable
    private LongStream scaleStream;

    public DecimalBlockReader(StreamDescriptor streamDescriptor, DecimalType type)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.type = checkNotNull(type, "type is null");
    }

    @Override
    public boolean readNextValueInto(BlockBuilder builder, boolean skipNull)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            if (!skipNull) {
                builder.appendNull();
                return true;
            }
            return false;
        }

        if (unboundedIntegerStream == null) {
            throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
        }
        if (scaleStream == null) {
            throw new OrcCorruptionException("Value is not null but scale stream is not present");
        }

        long scale = scaleStream.next();
        if (type.isShort()) {
            long unboundedInteger = unboundedIntegerStream.nextLong();
            unboundedInteger = DecimalStreamReader.rescale(unboundedInteger, (int) scale, type.getScale());
            type.writeLong(builder, unboundedInteger);
        }
        else {
            BigInteger unboundedInteger = unboundedIntegerStream.nextBigInteger();
            unboundedInteger = DecimalStreamReader.rescale(unboundedInteger, (int) scale, type.getScale());
            type.writeSlice(builder, LongDecimalType.unscaledValueToSlice(unboundedInteger));
        }

        return true;
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        if (skipSize == 0) {
            return;
        }

        if (unboundedIntegerStream == null) {
            throw new OrcCorruptionException("Value is not null but unbounded integer stream is not present");
        }
        if (scaleStream == null) {
            throw new OrcCorruptionException("Value is not null but scale stream is not present");
        }

        // skip non-null values
        unboundedIntegerStream.skip(skipSize);
        scaleStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        unboundedIntegerStream = null;
        scaleStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        unboundedIntegerStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, UnboundedIntegerStream.class).openStream();
        scaleStream = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class).openStream();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
