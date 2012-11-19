package com.facebook.presto.split;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.operator.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataStreamManager
    implements DataStreamProvider
{
    private final Map<DataSourceType, DataStreamProvider> dataStreamProviderMap;

    @Inject
    public DataStreamManager(
            NativeDataStreamProvider nativeProvider,
            ImportDataStreamProvider importProvider)
    {
        checkNotNull(nativeProvider, "nativeProvider is null");
        checkNotNull(importProvider, "importProvider is null");

        dataStreamProviderMap = ImmutableMap.<DataSourceType, DataStreamProvider>builder()
                .put(DataSourceType.NATIVE, nativeProvider)
                .put(DataSourceType.IMPORT, importProvider)
                .build();
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
