package com.facebook.presto.split;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataStreamManager
    implements DataStreamProvider
{
    private Map<DataSourceType, DataStreamProvider> dataStreamProviderMap;

    @Inject
    public DataStreamManager(
            NativeDataStreamProvider nativeProvider,
            InternalDataStreamProvider internalProvider,
            ImportDataStreamProvider importProvider)
    {
        checkNotNull(nativeProvider, "nativeProvider is null");
        checkNotNull(internalProvider, "internalProvider is null");
        checkNotNull(importProvider, "importProvider is null");

        dataStreamProviderMap = ImmutableMap.<DataSourceType, DataStreamProvider>builder()
                .put(DataSourceType.NATIVE, nativeProvider)
                .put(DataSourceType.INTERNAL, internalProvider)
                .put(DataSourceType.IMPORT, importProvider)
                .build();
    }

    // only used in TestDistributedQueries to get the Tpch stuff in.
    @Inject(optional = true)
    public synchronized void addTpchDataStreamProvider(TpchDataStreamProvider tpchDataStreamProvider)
    {
        ImmutableMap.Builder<DataSourceType, DataStreamProvider> builder = ImmutableMap.builder();
        builder.putAll(dataStreamProviderMap);
        builder.put(DataSourceType.TPCH, tpchDataStreamProvider);
        this.dataStreamProviderMap = builder.build();
    }

    private DataStreamProvider lookup(DataSourceType dataSourceType)
    {
        checkNotNull(dataSourceType, "dataSourceHandle is null");

        DataStreamProvider dataStreamProvider = dataStreamProviderMap.get(dataSourceType);
        checkArgument(dataStreamProvider != null, "dataStreamProvider does not exist: %s", dataSourceType);
        return dataStreamProvider;
    }

    @Override
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "no columns specified");

        return lookup(split.getDataSourceType()).createDataStream(split, columns);
    }
}
