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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

import static com.google.common.base.Preconditions.checkNotNull;

public class TpchHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    public TpchHandleResolver(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof TpchTableHandle && ((TpchTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof TpchColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof TpchSplit && ((TpchSplit) split).getTableHandle().getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(IndexHandle indexHandle)
    {
        return false;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return TpchTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return TpchColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return TpchSplit.class;
    }

    @Override
    public Class<? extends IndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }
}
