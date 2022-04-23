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
package com.facebook.presto.hive;

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorTypeSerde;
import com.facebook.presto.spi.connector.ConnectorTypeSerdeProvider;

import javax.inject.Inject;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HiveConnectorTypeSerdeProvider
        implements ConnectorTypeSerdeProvider
{
    private final ThriftCodecManager thriftCodecManager;
    private final Protocol thriftProtocol;
    private final int bufferSize;

    @Inject
    public HiveConnectorTypeSerdeProvider(HiveClientConfig hiveClientConfig, ThriftCodecManager thriftCodecManager)
    {
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.thriftProtocol = hiveClientConfig.getThriftProtocol();
        this.bufferSize = toIntExact(hiveClientConfig.getThriftBufferSize().toBytes());
    }

    @Override
    public ConnectorTypeSerde<ConnectorMetadataUpdateHandle> getConnectorMetadataUpdateHandleSerde()
    {
        return new HiveMetadataUpdateHandleThriftSerde(thriftCodecManager, thriftProtocol, bufferSize);
    }
}
