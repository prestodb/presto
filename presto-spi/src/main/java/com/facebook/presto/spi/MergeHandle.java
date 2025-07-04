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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class MergeHandle
{
    private final TableHandle tableHandle;
    private final ConnectorMergeTableHandle connectorMergeTableHandle;

    @JsonCreator
    public MergeHandle(
            @JsonProperty("tableHandle") TableHandle tableHandle,
            @JsonProperty("connectorMergeTableHandle") ConnectorMergeTableHandle connectorMergeTableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.connectorMergeTableHandle = requireNonNull(connectorMergeTableHandle, "connectorMergeTableHandle is null");
    }

    @JsonProperty
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public ConnectorMergeTableHandle getConnectorMergeTableHandle()
    {
        return connectorMergeTableHandle;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, connectorMergeTableHandle);
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
        MergeHandle o = (MergeHandle) obj;
        return Objects.equals(this.tableHandle, o.tableHandle) &&
                Objects.equals(this.connectorMergeTableHandle, o.connectorMergeTableHandle);
    }

    @Override
    public String toString()
    {
        return tableHandle + ":" + connectorMergeTableHandle;
    }
}
