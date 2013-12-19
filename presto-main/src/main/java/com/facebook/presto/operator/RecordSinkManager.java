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

import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.OutputTableHandle;
import com.facebook.presto.spi.RecordSink;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;

import static com.google.common.collect.Sets.newConcurrentHashSet;

public class RecordSinkManager
        implements RecordSinkProvider
{
    private final Set<ConnectorRecordSinkProvider> recordSinkProviders = newConcurrentHashSet();

    public RecordSinkManager(ConnectorRecordSinkProvider... recordSinkProviders)
    {
        this(ImmutableSet.copyOf(recordSinkProviders));
    }

    @Inject
    public RecordSinkManager(Set<ConnectorRecordSinkProvider> recordSinkProviders)
    {
        this.recordSinkProviders.addAll(recordSinkProviders);
    }

    public void addConnectorRecordSinkProvider(ConnectorRecordSinkProvider recordSinkProvider)
    {
        recordSinkProviders.add(recordSinkProvider);
    }

    @Override
    public RecordSink getRecordSink(OutputTableHandle tableHandle)
    {
        for (ConnectorRecordSinkProvider provider : recordSinkProviders) {
            if (provider.canHandle(tableHandle)) {
                return provider.getRecordSink(tableHandle);
            }
        }
        throw new IllegalArgumentException("No record sink for " + tableHandle);
    }
}
