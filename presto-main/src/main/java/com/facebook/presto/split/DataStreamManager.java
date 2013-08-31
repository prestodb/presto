package com.facebook.presto.split;

import com.facebook.presto.noperator.NewOperator;
import com.facebook.presto.noperator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataStreamManager
        implements DataStreamProvider
{
    private final Set<ConnectorDataStreamProvider> dataStreamProviders = Sets.newSetFromMap(new ConcurrentHashMap<ConnectorDataStreamProvider, Boolean>());

    public DataStreamManager(ConnectorDataStreamProvider... dataStreamProviders)
    {
        this(ImmutableSet.copyOf(dataStreamProviders));
    }

    @Inject
    public DataStreamManager(Set<ConnectorDataStreamProvider> dataStreamProviders)
    {
        this.dataStreamProviders.addAll(dataStreamProviders);
    }

    public void addConnectorDataStreamProvider(ConnectorDataStreamProvider connectorDataStreamProvider)
    {
        dataStreamProviders.add(connectorDataStreamProvider);
    }

    @Override
    public NewOperator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        checkNotNull(operatorContext, "operatorContext is null");
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "no columns specified");

        return getDataStreamProvider(split).createNewDataStream(operatorContext, split, columns);
    }

    private ConnectorDataStreamProvider getDataStreamProvider(Split split)
    {
        for (ConnectorDataStreamProvider dataStreamProvider : dataStreamProviders) {
            if (dataStreamProvider.canHandle(split)) {
                return dataStreamProvider;
            }
        }
        throw new IllegalArgumentException("No data stream provider for " + split);
    }

}
