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
import com.facebook.presto.spi.PrestoException;

import java.lang.reflect.Type;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.thrift.codec.ThriftCodecUtils.fromThrift;
import static com.facebook.presto.thrift.codec.ThriftCodecUtils.toThrift;
import static java.util.Objects.requireNonNull;

public class GenericThriftCodec<T>
        implements ConnectorCodec<T>
{
    private final ThriftCodec<T> thriftCodec;

    public GenericThriftCodec(ThriftCodecManager codecManager, Type javaType)
    {
        requireNonNull(codecManager, "codecManager is null");
        requireNonNull(javaType, "javaType is null");

        if (!(javaType instanceof Class<?>)) {
            throw new IllegalArgumentException("Expected a Class type for javaType, but got: " + javaType.getTypeName());
        }

        Class<?> clazz = (Class<?>) javaType;

        this.thriftCodec = (ThriftCodec<T>) codecManager.getCodec(clazz);
    }

    @Override
    public byte[] serialize(T value)
    {
        try {
            return toThrift(value, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Unable to serialize object of type " + value.getClass().getSimpleName(), e);
        }
    }

    @Override
    public T deserialize(byte[] bytes)
    {
        try {
            return fromThrift(bytes, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Unable to deserialize bytes to object of expected type", e);
        }
    }
}
