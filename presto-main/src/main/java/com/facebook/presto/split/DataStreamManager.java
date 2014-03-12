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
package com.facebook.presto.split;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.google.common.collect.Lists;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.ColumnHandle.connectorHandleGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DataStreamManager
        implements DataStreamProvider
{
    private final ConcurrentMap<String, ConnectorDataStreamProvider> dataStreamProviders = new ConcurrentHashMap<>();

    @Inject
    public DataStreamManager()
    {
    }

    public void addConnectorDataStreamProvider(String connectorId, ConnectorDataStreamProvider connectorDataStreamProvider)
    {
        dataStreamProviders.put(connectorId, connectorDataStreamProvider);
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        checkNotNull(operatorContext, "operatorContext is null");
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "no columns specified");

        List<ConnectorColumnHandle> handles = Lists.transform(columns, connectorHandleGetter());

        return getDataStreamProvider(split).createNewDataStream(operatorContext, split.getConnectorSplit(), handles);
    }

    private ConnectorDataStreamProvider getDataStreamProvider(Split split)
    {
        ConnectorDataStreamProvider provider = dataStreamProviders.get(split.getConnectorId());

        checkArgument(provider != null, "No data stream provider for '%s", split.getConnectorId());

        return provider;
    }
}
