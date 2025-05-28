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
import com.facebook.presto.spi.ConnectorSpecificCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorSpecificCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HiveConnectorCodecProvider
        implements ConnectorSpecificCodecProvider
{
    private final ThriftCodecManager thriftCodecManager;

    @Inject
    public HiveConnectorCodecProvider(ThriftCodecManager thriftCodecManager)
    {
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
    }

    @Override
    public ConnectorSpecificCodec<ConnectorSplit> getConnectorSplitCodec()
    {
        return new HiveSplitCodec(thriftCodecManager);
    }

    @Override
    public ConnectorSpecificCodec<ConnectorTransactionHandle> getConnectorTransactionHandleCodec()
    {
        return new HiveTransactionHandleCodec(thriftCodecManager);
    }
}
