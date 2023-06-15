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
package com.facebook.presto.server;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import javax.inject.Inject;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class LongSetCodec
        implements ThriftCodec<LongSet>
{
    @Inject
    public LongSetCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.BINARY, LongSet.class);
    }

    @Override
    public LongSet read(TProtocolReader protocol)
            throws Exception
    {
        ByteBuffer buffer = protocol.readBinary();
        return bytesToLongSet(buffer);
    }

    @Override
    public void write(LongSet value, TProtocolWriter protocol)
            throws Exception
    {
        protocol.writeBinary(longSetToBytes(value));
    }

    @SuppressWarnings("checkstyle:InnerAssignment")
    @FromThrift
    public static LongSet bytesToLongSet(ByteBuffer buffer)
    {
        requireNonNull(buffer, "buffer is null");
        int length;
        if (buffer.remaining() == 0 || (length = buffer.getInt()) == 0) {
            return new LongOpenHashSet();
        }
        long[] longs = new long[length];
        buffer.asLongBuffer().get(longs);
        return new LongOpenHashSet(longs);
    }

    @ToThrift
    public static ByteBuffer longSetToBytes(LongSet value)
    {
        requireNonNull(value, "value is null");
        ByteBuffer bb = ByteBuffer.allocate(value.size() * Long.BYTES + Integer.BYTES);
        bb.putInt(value.size());
        bb.asLongBuffer().put(value.toLongArray());
        bb.position(0);
        return bb;
    }
}
