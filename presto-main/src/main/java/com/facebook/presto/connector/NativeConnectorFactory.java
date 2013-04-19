package com.facebook.presto.connector;

import com.facebook.presto.metadata.ConnectorHandleResolver;
import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.NativeHandleResolver;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.ConnectorSplitManager;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.NativeSplitManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableClassToInstanceMap;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class NativeConnectorFactory
        implements ConnectorFactory
{
    private static final NativeHandleResolver HANDLE_RESOLVER = new NativeHandleResolver();

    private final IDBI dbi;
    private final NativeSplitManager splitManager;
    private final NativeDataStreamProvider dataStreamProvider;

    @Inject
    public NativeConnectorFactory(@ForMetadata IDBI dbi, NativeSplitManager splitManager, NativeDataStreamProvider dataStreamProvider)
            throws InterruptedException
    {
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        NativeMetadata nativeMetadata;
        try {
            nativeMetadata = new NativeMetadata(connectorId, dbi);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }

        ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
        builder.put(ConnectorMetadata.class, nativeMetadata);
        builder.put(ConnectorSplitManager.class, splitManager);
        builder.put(ConnectorDataStreamProvider.class, dataStreamProvider);
        builder.put(ConnectorHandleResolver.class, HANDLE_RESOLVER);

        return new StaticConnector(builder.build());
    }
}
