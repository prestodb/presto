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
package com.facebook.presto.operator;

import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;

public class RecordSinkManager
        implements RecordSinkProvider
{
    private final ConcurrentMap<String, ConnectorRecordSinkProvider> recordSinkProviders = new ConcurrentHashMap<>();

    public void addConnectorRecordSinkProvider(String connectorId, ConnectorRecordSinkProvider recordSinkProvider)
    {
        ConnectorRecordSinkProvider previous = recordSinkProviders.putIfAbsent(connectorId, recordSinkProvider);
        checkArgument(previous == null, "Record sink provider already registered for connector '%s'", connectorId);
    }

    @Override
    public RecordSink getRecordSink(OutputTableHandle tableHandle)
    {
        ConnectorRecordSinkProvider provider = recordSinkProviders.get(tableHandle.getConnectorId());

        checkArgument(provider != null, "No record sink for '%s'", tableHandle.getConnectorId());

        return provider.getRecordSink(tableHandle.getConnectorHandle());
    }
}
