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
package com.facebook.presto.metadata;

import com.facebook.airlift.http.client.thrift.ThriftProtocolException;
import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.ConnectorHandleSerde;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static java.lang.String.format;

public class ConnectorMetadataUpdateHandleSerde<T>
        implements ConnectorHandleSerde<T>
{
    private final ThriftCodecManager thriftCodecManager;
    private final Protocol thriftProtocol;

    public ConnectorMetadataUpdateHandleSerde(
            ThriftCodecManager thriftCodecManager,
            Protocol thriftProtocol)
    {
        this.thriftCodecManager = thriftCodecManager;
        this.thriftProtocol = thriftProtocol;
    }

    @Override
    public byte[] serialize(T val)
    {
        ThriftCodec codec = thriftCodecManager.getCodec(val.getClass());
        //Allocating dynamic Slice with initial size of 128 bytes.
        //This can be tuned and made a config in the future.
        SliceOutput dynamicSliceOutput = new DynamicSliceOutput(128);
        try {
            ThriftProtocolUtils.write(val, codec, thriftProtocol, dynamicSliceOutput);
            return dynamicSliceOutput.slice().getBytes();
        }
        catch (ThriftProtocolException e) {
            throw new IllegalArgumentException(format("%s could not be converted to Thrift", val.getClass().getName()), e);
        }
    }

    @Override
    public T deSerialize(Class<? extends T> connectorHandleClass, byte[] byteArr)
    {
        try {
            ThriftCodec<? extends T> codec = thriftCodecManager.getCodec(connectorHandleClass);
            return ThriftProtocolUtils.read(codec, thriftProtocol, Slices.wrappedBuffer(byteArr).getInput());
        }
        catch (ThriftProtocolException e) {
            throw new IllegalArgumentException(format("Invalid Thrift bytes for %s", connectorHandleClass), e);
        }
    }
}
