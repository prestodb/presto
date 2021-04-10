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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;

import java.util.List;

public interface ConnectorMetadataUpdater
{
    /**
     * Gets the pending metadata update requests that are to be sent to the coordinator.
     *
     * @return List of pending metadata update requests
     */
    List<ConnectorMetadataUpdateHandle> getPendingMetadataUpdateRequests();

    /**
     * Sets the metadata update results received from the coordinator.
     *
     * @param results List of metadata update results
     */
    void setMetadataUpdateResults(List<ConnectorMetadataUpdateHandle> results);
}
