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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TableHandle
{
    private final String connectorId;
    private final ConnectorTableHandle connectorHandle;

    @JsonCreator
    public TableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("connectorHandle") ConnectorTableHandle connectorHandle)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.connectorHandle = checkNotNull(connectorHandle, "connectorHandle is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorTableHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, connectorHandle);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TableHandle other = (TableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.connectorHandle, other.connectorHandle);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + connectorHandle;
    }
}
