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
package com.facebook.presto.connector;

import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.NativeHandleResolver;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.NativeRecordSinkProvider;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.NativeSplitManager;
import com.google.common.collect.ImmutableClassToInstanceMap;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NativeConnectorFactory
        implements ConnectorFactory
{
    private static final NativeHandleResolver HANDLE_RESOLVER = new NativeHandleResolver();

    private final AtomicReference<IDBI> dbi = new AtomicReference<>();
    private final AtomicReference<ShardManager> shardManager = new AtomicReference<>();
    private final AtomicReference<NativeSplitManager> splitManager = new AtomicReference<>();
    private final NativeDataStreamProvider dataStreamProvider;
    private final NativeRecordSinkProvider recordSinkProvider;

    @Inject
    public NativeConnectorFactory(NativeDataStreamProvider dataStreamProvider, NativeRecordSinkProvider recordSinkProvider)
    {
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.recordSinkProvider = checkNotNull(recordSinkProvider, "recordSinkProvider is null");
    }

    @Inject
    public void setDbi(@ForMetadata IDBI dbi)
    {
        this.dbi.set(checkNotNull(dbi, "dbi is null"));
    }

    @Inject
    public void setShardManager(ShardManager shardManager)
    {
        this.shardManager.set(checkNotNull(shardManager, "shardManager is null"));
    }

    @Inject
    public void setSplitManager(NativeSplitManager splitManager)
    {
        this.splitManager.set(checkNotNull(splitManager, "splitManager is null"));
    }

    @Override
    public String getName()
    {
        return "native";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();

        IDBI dbi = this.dbi.get();
        if (dbi != null) {
            checkState(shardManager.get() != null, "shardManager was not set");
            builder.put(ConnectorMetadata.class, new NativeMetadata(connectorId, dbi, shardManager.get()));
        }

        NativeSplitManager splitManager = this.splitManager.get();
        if (splitManager != null) {
            builder.put(ConnectorSplitManager.class, splitManager);
        }

        builder.put(ConnectorDataStreamProvider.class, dataStreamProvider);
        builder.put(ConnectorHandleResolver.class, HANDLE_RESOLVER);

        builder.put(ConnectorRecordSinkProvider.class, recordSinkProvider);
        builder.put(ConnectorOutputHandleResolver.class, HANDLE_RESOLVER);

        return new StaticConnector(builder.build());
    }
}
