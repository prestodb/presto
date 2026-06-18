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

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TMemoryBufferWriteOnly;
import com.facebook.drift.protocol.TProtocolException;

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
}
