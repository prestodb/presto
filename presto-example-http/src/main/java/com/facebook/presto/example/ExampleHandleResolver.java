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
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExampleHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    @Inject
    public ExampleHandleResolver(ExampleConnectorId clientId)
    {
        this.connectorId = checkNotNull(clientId, "clientId is null").toString();
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof ExampleTableHandle && ((ExampleTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof ExampleColumnHandle && ((ExampleColumnHandle) columnHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof ExampleSplit && ((ExampleSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(IndexHandle indexHandle)
    {
        return false;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return ExampleTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return ExampleColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return ExampleSplit.class;
    }

    @Override
    public Class<? extends IndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }
}
