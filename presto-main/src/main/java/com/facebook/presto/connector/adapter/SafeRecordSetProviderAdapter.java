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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SafeRecordSetProvider;

import java.util.List;

public class SafeRecordSetProviderAdapter<CH extends ConnectorColumnHandle, S extends ConnectorSplit>
        implements ConnectorRecordSetProvider
{
    private final SafeRecordSetProvider<CH, S> delegate;
    private final SafeTypeAdapter<?, CH, S, ?, ?, ?, ?> typeAdapter;

    public SafeRecordSetProviderAdapter(SafeRecordSetProvider<CH, S> delegate, SafeTypeAdapter<?, CH, S, ?, ?, ?, ?> typeAdapter)
    {
        this.delegate = delegate;
        this.typeAdapter = typeAdapter;
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        return delegate.getRecordSet(typeAdapter.castSplit(split), typeAdapter.castColumnHandles(columns));
    }
}
