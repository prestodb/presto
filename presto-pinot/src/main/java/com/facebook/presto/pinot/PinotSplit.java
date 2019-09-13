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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PinotSplit
        implements ConnectorSplit
{
    private static final Logger log = Logger.get(PinotSplit.class);

    private final String connectorId;
    private final String tableName;
    private final String host;
    private final String segment;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;
    private final PinotColumn timeColumn;
    private final String timeFilter;
    private final String pinotFilter;
    private final long limit;

    @JsonCreator
    public PinotSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("host") String host,
            @JsonProperty("segment") String segment,
            @JsonProperty("timeColumn") PinotColumn timeColumn,
            @JsonProperty("timeFilter") String timeFilter,
            @JsonProperty("pinotFilter") String pinotFilter,
            @JsonProperty("limit") long limit)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.host = requireNonNull(host, "host is null");
        this.segment = requireNonNull(segment, "segment is null");
        this.timeColumn = requireNonNull(timeColumn, "timeColumn is null");
        this.addresses = null;
        this.pinotFilter = pinotFilter;
        this.timeFilter = timeFilter;
        this.limit = limit;
        this.remotelyAccessible = true;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getHost()
    {
        return host;
    }

    @JsonProperty
    public String getSegment()
    {
        return segment;
    }

    @JsonProperty
    public PinotColumn getTimeColumn()
    {
        return timeColumn;
    }

    @JsonProperty
    public String getTimeFilter()
    {
        return timeFilter;
    }

    @JsonProperty
    public String getPinotFilter()
    {
        return pinotFilter;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
