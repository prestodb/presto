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
package com.facebook.presto.execution;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdater;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskMetadataContext
{
    private final List<ConnectorMetadataUpdater> metadataUpdaters;
    private ConnectorId connectorId;

    public TaskMetadataContext()
    {
        this.metadataUpdaters = new CopyOnWriteArrayList<>();
    }

    public void setConnectorId(ConnectorId connectorId)
    {
        this.connectorId = connectorId;
    }

    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    public void addMetadataUpdater(ConnectorMetadataUpdater metadataUpdater)
    {
        metadataUpdaters.add(metadataUpdater);
    }

    public List<ConnectorMetadataUpdater> getMetadataUpdaters()
    {
        return metadataUpdaters;
    }
}
