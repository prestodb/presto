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
package com.facebook.presto.tpcds.thrift;

import com.facebook.drift.TException;
import com.facebook.drift.buffer.ByteBufferPool;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TMemoryBufferWriteOnly;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.bytebuffer.ByteBufferInputTransport;
import com.facebook.drift.protocol.bytebuffer.ByteBufferOutputTransport;
import com.facebook.presto.common.thrift.ByteBufferPoolManager;
import com.facebook.presto.spi.ConnectorCodec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ThriftCodecUtils
{
    private ThriftCodecUtils() {}

    public static <T> T fromThrift(byte[] bytes, ThriftCodec<T> thriftCodec)
            throws TProtocolException
    {
        try {
            TMemoryBuffer transport = new TMemoryBuffer(bytes.length);
            transport.write(bytes);
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            return thriftCodec.read(protocol);
        }
        catch (Exception e) {
            throw new TProtocolException("Can not deserialize the data", e);
        }
    }

    public static <T> byte[] toThrift(T value, ThriftCodec<T> thriftCodec)
            throws TProtocolException
    {
        TMemoryBufferWriteOnly transport = new TMemoryBufferWriteOnly(1024);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        try {
            thriftCodec.write(value, protocol);
            return transport.getBytes();
        }
        catch (Exception e) {
            throw new TProtocolException("Can not serialize the data", e);
        }
    }

    public static <T> T deserializeConcreteValue(
            List<ByteBuffer> byteBuffers,
            ThriftCodec<T> codec)
            throws Exception
    {
        ByteBufferInputTransport transport = new ByteBufferInputTransport(byteBuffers);
        TProtocol protocol = new TBinaryProtocol(transport);
        return codec.read(protocol);
    }

    public static <T> void serializeConcreteValue(T value, ThriftCodec<T> codec, ByteBufferPool pool, Consumer<List<ByteBuffer>> consumer)
            throws Exception
    {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        ByteBufferOutputTransport transport = new ByteBufferOutputTransport(pool, byteBuffers);
        TProtocol protocol = new TBinaryProtocol(transport);

        codec.write(value, protocol);
        transport.finish();
        consumer.accept(byteBuffers);
    }

    public static <T> T deserialize(ConnectorCodec<T> codec, TProtocolReader reader, ByteBufferPoolManager byteBufferPoolManager)
            throws Exception
    {
        ByteBufferPool byteBufferPool = byteBufferPoolManager.getPool();
        List<ByteBuffer> byteBuffers = reader.readBinaryToBuffers(byteBufferPool);
        if (byteBuffers.isEmpty()) {
            return null;
        }

        try {
            return codec.deserialize(byteBuffers);
        }
        finally {
            for (ByteBuffer byteBuffer : byteBuffers) {
                byteBufferPool.release(byteBuffer);
            }
        }
    }

    public static <T> void serialize(ConnectorCodec<T> codec, T value, TProtocolWriter writer, ByteBufferPoolManager byteBufferPoolManager)
            throws Exception
    {
        codec.serialize(value, byteBuffers -> {
            try {
                writer.writeBinaryFromBuffers(byteBuffers);
            }
            catch (TException e) {
                throw new IllegalStateException("Failed to serialize value", e);
            }
            finally {
                for (ByteBuffer byteBuffer : byteBuffers) {
                    byteBufferPoolManager.getPool().release(byteBuffer);
                }
            }
        });
    }
}
