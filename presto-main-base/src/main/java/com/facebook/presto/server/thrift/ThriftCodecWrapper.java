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
package com.facebook.presto.server.thrift;

import com.facebook.airlift.http.client.thrift.ThriftProtocolException;
import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.airlift.json.Codec;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.io.InputStream;
import java.io.OutputStream;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ThriftCodecWrapper<T>
        implements Codec<T>
{
    private final ThriftCodec<T> thriftCodec;

    public ThriftCodecWrapper(ThriftCodec<T> thriftCodec)
    {
        this.thriftCodec = requireNonNull(thriftCodec, "thriftCodec is null");
    }

    public static <T> ThriftCodecWrapper<T> wrapThriftCodec(ThriftCodec<T> codec)
    {
        return new ThriftCodecWrapper<>(codec);
    }

    public static <T> ThriftCodec<T> unwrapThriftCodec(Codec<T> codec)
    {
        verify(codec instanceof ThriftCodecWrapper);
        return ((ThriftCodecWrapper<T>) codec).thriftCodec;
    }

    @Override
    public byte[] toBytes(T instance)
    {
        try {
            SliceOutput sliceOutput = new DynamicSliceOutput(1024);
            ThriftProtocolUtils.write(instance, thriftCodec, Protocol.BINARY, sliceOutput);
            return sliceOutput.slice().getBytes();
        }
        catch (ThriftProtocolException e) {
            throw new PrestoException(NOT_SUPPORTED, "Can not serialize instance to bytes", e);
        }
    }

    @Override
    public T fromBytes(byte[] bytes)
    {
        try {
            return ThriftProtocolUtils.read(thriftCodec, Protocol.BINARY, Slices.wrappedBuffer(bytes).getInput());
        }
        catch (ThriftProtocolException e) {
            throw new PrestoException(NOT_SUPPORTED, "Can not deserialize instance from bytes", e);
        }
    }

    @Override
    public void writeBytes(OutputStream output, T instance)
    {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public T readBytes(InputStream input)
    {
        throw new UnsupportedOperationException("Operation not supported");
    }
}
