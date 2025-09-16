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
package com.facebook.presto.thrift.codec;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.PrestoException;

import java.lang.reflect.Type;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.thrift.codec.ThriftCodecUtils.fromThrift;
import static com.facebook.presto.thrift.codec.ThriftCodecUtils.toThrift;
import static java.util.Objects.requireNonNull;

public class ThriftDeleteTableHandleCodec
        implements ConnectorCodec<ConnectorDeleteTableHandle>
{
    private final ThriftCodec<ConnectorDeleteTableHandle> thriftCodec;

    public ThriftDeleteTableHandleCodec(ThriftCodecManager thriftCodecManager, Type deleteTableLayoutHandle)
    {
        if (!(deleteTableLayoutHandle instanceof Class<?>)) {
            throw new IllegalArgumentException("Expected a Class type for javaType, but got: " + deleteTableLayoutHandle.getTypeName());
        }

        Class<?> clazz = (Class<?>) deleteTableLayoutHandle;
        if (!ConnectorOutputTableHandle.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("javaType must be a subclass of ConnectorDeleteTableHandle, but got: " + clazz.getName());
        }

        this.thriftCodec = (ThriftCodec<ConnectorDeleteTableHandle>) requireNonNull(thriftCodecManager, "thriftCodecManager is null").getCodec(clazz);
    }

    @Override
    public byte[] serialize(ConnectorDeleteTableHandle split)
    {
        try {
            return toThrift(split, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not serialize tpcds split", e);
        }
    }

    @Override
    public ConnectorDeleteTableHandle deserialize(byte[] bytes)
    {
        try {
            return fromThrift(bytes, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not deserialize tpcds split", e);
        }
    }
}
