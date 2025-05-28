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
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveCodecProvider
        implements ConnectorCodecProvider
{
    private final ThriftCodecManager thriftCodecManager;

    @Inject
    public HiveCodecProvider(ThriftCodecManager thriftCodecManager)
    {
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.of(new HiveSplitCodec(thriftCodecManager));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.of(new HiveTransactionHandleCodec(thriftCodecManager));
    }
}
