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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class Split
{
    private final String connectorId;
    private final ConnectorSplit connectorSplit;

    @JsonCreator
    public Split(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.connectorSplit = checkNotNull(connectorSplit, "connectorSplit is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    public Object getInfo()
    {
        return connectorSplit.getInfo();
    }

    public List<HostAddress> getAddresses()
    {
        return connectorSplit.getAddresses();
    }

    public boolean isRemotelyAccessible()
    {
        return connectorSplit.isRemotelyAccessible();
    }

    public static Function<ConnectorSplit, Split> fromConnectorSplit(final String connectorId)
    {
        return new Function<ConnectorSplit, Split>() {
            @Override
            public Split apply(ConnectorSplit split)
            {
                return new Split(connectorId, split);
            }
        };
    }
}
