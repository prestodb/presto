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
package io.prestosql.orc.stream;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.orc.OrcOutputBuffer;
import io.prestosql.orc.checkpoint.DecimalStreamCheckpoint;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.spi.type.Decimals;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.stream.LongDecode.writeVLong;
import static java.lang.Math.toIntExact;

/**
 * This is only for mantissa/significant of a decimal and not the exponent.
 */
public class DecimalOutputStream
        implements ValueOutputStream<DecimalStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalOutputStream.class).instanceSize();
    private final OrcOutputBuffer buffer;
    private final List<DecimalStreamCheckpoint> checkpoints = new ArrayList<>();

    private boolean closed;

    public DecimalOutputStream(CompressionKind compression, int bufferSize)
    {
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    // todo rewrite without BigInteger
    // This comes from the Apache Hive ORC code
    public void writeUnscaledValue(Slice slice)
    {
        BigInteger value = Decimals.decodeUnscaledValue(slice);

        // encode the signed number as a positive integer
        value = value.shiftLeft(1);
        int sign = value.signum();
        if (sign < 0) {
            value = value.negate();
            value = value.subtract(BigInteger.ONE);
        }
        int length = value.bitLength();
        while (true) {
            long lowBits = value.longValue() & 0x7fff_ffff_ffff_ffffL;
            length -= 63;
            // write out the next 63 bits worth of data
            for (int i = 0; i < 9; ++i) {
                // if this is the last byte, leave the high bit off
                if (length <= 0 && (lowBits & ~0x7f) == 0) {
                    buffer.write((byte) lowBits);
                    return;
                }
                else {
                    buffer.write((byte) (0x80 | (lowBits & 0x7f)));
                    lowBits >>>= 7;
                }
            }
            value = value.shiftRight(63);
        }
    }

    public void writeUnscaledValue(long value)
    {
        checkState(!closed);
        writeVLong(buffer, value, true);
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new DecimalStreamCheckpoint(buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
        buffer.close();
    }

    @Override
    public List<DecimalStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public StreamDataOutput getStreamDataOutput(int column)
    {
        return new StreamDataOutput(buffer::writeDataTo, new Stream(column, DATA, toIntExact(buffer.getOutputDataSize()), true));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.estimateOutputDataSize();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + buffer.getRetainedSize();
    }

    @Override
    public void reset()
    {
        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}
