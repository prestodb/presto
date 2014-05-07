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

import com.facebook.presto.spi.ConnectorIndexHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class IndexHandle
{
    private final String connectorId;
    private final ConnectorIndexHandle connectorHandle;

    @JsonCreator
    public IndexHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("connectorHandle") ConnectorIndexHandle connectorHandle)
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
    public ConnectorIndexHandle getConnectorHandle()
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
        final IndexHandle other = (IndexHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.connectorHandle, other.connectorHandle);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + connectorHandle;
    }

    public static Function<IndexHandle, ConnectorIndexHandle> connectorHandleGetter()
    {
        return new Function<IndexHandle, ConnectorIndexHandle>()
        {
            @Override
            public ConnectorIndexHandle apply(IndexHandle input)
            {
                return input.getConnectorHandle();
            }
        };
    }
}
