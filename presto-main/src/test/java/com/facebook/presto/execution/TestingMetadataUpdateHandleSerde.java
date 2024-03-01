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
package com.facebook.presto.execution;

import com.facebook.airlift.http.client.thrift.ThriftProtocolException;
import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorTypeSerde;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingMetadataUpdateHandleSerde
        implements ConnectorTypeSerde<ConnectorMetadataUpdateHandle>
{
    private final ThriftCodecManager thriftCodecManager;
    private final Protocol thriftProtocol;
    private final int bufferSize;

    public TestingMetadataUpdateHandleSerde(
            ThriftCodecManager thriftCodecManager,
            Protocol thriftProtocol,
            int bufferSize)
    {
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
        this.thriftProtocol = requireNonNull(thriftProtocol, "thriftProtocol is null");
        this.bufferSize = bufferSize;
    }

    @Override
    public byte[] serialize(ConnectorMetadataUpdateHandle value)
    {
        ThriftCodec codec = thriftCodecManager.getCodec(value.getClass());
        SliceOutput dynamicSliceOutput = new DynamicSliceOutput(bufferSize);
        try {
            ThriftProtocolUtils.write(value, codec, thriftProtocol, dynamicSliceOutput);
            return dynamicSliceOutput.slice().getBytes();
        }
        catch (ThriftProtocolException e) {
            throw new IllegalArgumentException(format("%s could not be converted to Thrift", value.getClass().getName()), e);
        }
    }

    @Override
    public ConnectorMetadataUpdateHandle deserialize(Class<? extends ConnectorMetadataUpdateHandle> connectorTypeClass, byte[] bytes)
    {
        try {
            ThriftCodec<? extends ConnectorMetadataUpdateHandle> codec = thriftCodecManager.getCodec(connectorTypeClass);
            return ThriftProtocolUtils.read(codec, thriftProtocol, Slices.wrappedBuffer(bytes).getInput());
        }
        catch (ThriftProtocolException e) {
            throw new IllegalArgumentException(format("Invalid Thrift bytes for %s", connectorTypeClass), e);
        }
    }
}
