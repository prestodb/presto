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
package com.facebook.presto.server.thrift.codec;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidThriftCodec
        implements ThriftCodec<UUID>
{
    @Inject
    public UuidThriftCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.BINARY, UUID.class);
    }

    @Override
    public UUID read(TProtocolReader protocol)
            throws Exception
    {
        return byteToUuid(protocol.readBinary());
    }

    @Override
    public void write(UUID uuid, TProtocolWriter protocol)
            throws Exception
    {
        protocol.writeBinary(uuidToBytes(uuid));
    }

    @FromThrift
    public static UUID byteToUuid(ByteBuffer bytes)
    {
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    @ToThrift
    public static ByteBuffer uuidToBytes(UUID uuid)
    {
        ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES * 2);
        bytes.putLong(uuid.getLeastSignificantBits());
        bytes.putLong(uuid.getMostSignificantBits());
        bytes.position(0);
        return bytes;
    }
}
