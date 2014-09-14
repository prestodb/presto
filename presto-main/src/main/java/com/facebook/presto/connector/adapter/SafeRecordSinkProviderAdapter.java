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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SafeRecordSinkProvider;

public class SafeRecordSinkProviderAdapter<OTH extends ConnectorOutputTableHandle, ITH extends ConnectorInsertTableHandle>
        implements ConnectorRecordSinkProvider
{
    private final SafeRecordSinkProvider<OTH, ITH> delegate;
    private final SafeTypeAdapter<?, ?, ?, ?, OTH, ITH, ?> typeAdapter;

    public SafeRecordSinkProviderAdapter(SafeRecordSinkProvider<OTH, ITH> delegate, SafeTypeAdapter<?, ?, ?, ?, OTH, ITH, ?> typeAdapter)
    {
        this.delegate = delegate;
        this.typeAdapter = typeAdapter;
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle)
    {
        return delegate.getRecordSink(typeAdapter.castOutputTableHandle(tableHandle));
    }

    @Override
    public RecordSink getRecordSink(ConnectorInsertTableHandle tableHandle)
    {
        return delegate.getRecordSink(typeAdapter.castInsertTableHandle(tableHandle));
    }
}
