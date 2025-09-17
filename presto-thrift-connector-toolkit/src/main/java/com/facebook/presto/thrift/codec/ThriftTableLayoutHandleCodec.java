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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;

import java.lang.reflect.Type;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.thrift.codec.ThriftCodecUtils.fromThrift;
import static com.facebook.presto.thrift.codec.ThriftCodecUtils.toThrift;
import static java.util.Objects.requireNonNull;

public class ThriftTableLayoutHandleCodec
        implements ConnectorCodec<ConnectorTableLayoutHandle>
{
    private final ThriftCodec<ConnectorTableLayoutHandle> thriftCodec;

    public ThriftTableLayoutHandleCodec(ThriftCodecManager thriftCodecManager, Type connectorTableLayoutHandle)
    {
        if (!(connectorTableLayoutHandle instanceof Class<?>)) {
            throw new IllegalArgumentException("Expected a Class type for javaType, but got: " + connectorTableLayoutHandle.getTypeName());
        }

        Class<?> clazz = (Class<?>) connectorTableLayoutHandle;
        if (!ConnectorTableLayoutHandle.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("javaType must be a subclass of ConnectorTableLayoutHandle, but got: " + clazz.getName());
        }
        this.thriftCodec = (ThriftCodec<ConnectorTableLayoutHandle>) requireNonNull(thriftCodecManager, "thriftCodecManager is null").getCodec(clazz);
    }

    @Override
    public byte[] serialize(ConnectorTableLayoutHandle handle)
    {
        try {
            return toThrift(handle, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not serialize tpcds table Layout handle", e);
        }
    }

    @Override
    public ConnectorTableLayoutHandle deserialize(byte[] bytes)
    {
        try {
            return fromThrift(bytes, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not deserialize tpcds table Layout handle", e);
        }
    }
}
