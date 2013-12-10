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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.IndexHandle;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.split.NativeSplit;

public class NativeHandleResolver
        implements ConnectorHandleResolver, ConnectorOutputHandleResolver
{
    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof NativeTableHandle;
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle instanceof NativeColumnHandle;
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof NativeSplit;
    }

    @Override
    public boolean canHandle(OutputTableHandle tableHandle)
    {
        return tableHandle instanceof NativeOutputTableHandle;
    }

    @Override
    public boolean canHandle(IndexHandle indexHandle)
    {
        return false;
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        return NativeTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return NativeColumnHandle.class;
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        return NativeSplit.class;
    }

    @Override
    public Class<? extends OutputTableHandle> getOutputTableHandleClass()
    {
        return NativeOutputTableHandle.class;
    }

    @Override
    public Class<? extends IndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }
}
