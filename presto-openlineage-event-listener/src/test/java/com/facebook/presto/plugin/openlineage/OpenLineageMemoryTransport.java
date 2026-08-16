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
package com.facebook.presto.plugin.openlineage;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OpenLineageMemoryTransport
        extends Transport
{
    private final List<OpenLineage.BaseEvent> processedEvents = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void emit(OpenLineage.RunEvent runEvent)
    {
        processedEvents.add(runEvent);
    }

    @Override
    public void emit(OpenLineage.DatasetEvent datasetEvent)
    {
        processedEvents.add(datasetEvent);
    }

    @Override
    public void emit(OpenLineage.JobEvent jobEvent)
    {
        processedEvents.add(jobEvent);
    }

    public void clearProcessedEvents()
    {
        processedEvents.clear();
    }

    public List<OpenLineage.BaseEvent> getProcessedEvents()
    {
        return ImmutableList.copyOf(processedEvents);
    }
}
