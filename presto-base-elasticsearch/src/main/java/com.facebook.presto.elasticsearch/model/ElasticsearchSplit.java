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
package com.facebook.presto.elasticsearch.model;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schema;
    private final String table;
    private final List<HostAddress> addresses;

    private final byte[] searchRequest;
    private final long timeValue;
    private final Map<String, String> pushDownDsl;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("searchRequest") byte[] searchRequest,
            @JsonProperty("timeValue") long timeValue,
            @JsonProperty("pushDownDsl") Map<String, String> pushDownDsl,
            @JsonProperty("hostPort") Optional<String> hostPort)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.searchRequest = requireNonNull(searchRequest, "searchRequest is null");
        this.timeValue = timeValue;
        this.pushDownDsl = requireNonNull(pushDownDsl, "pushDownDsl is null");

        // Parse the host address into a list of addresses, this would be an Elasticsearch Tablet server or some localhost thing
        if (hostPort.isPresent()) {
            addresses = ImmutableList.of(HostAddress.fromString(hostPort.get()));
        }
        else {
            addresses = ImmutableList.of();
        }
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public byte[] getSearchRequest()
    {
        return searchRequest;
    }

    @JsonProperty
    public long getTimeValue()
    {
        return timeValue;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonIgnore
    public String getFullTableName()
    {
        return (this.getSchema().equals("default") ? "" : this.getSchema() + ".") + this.getTable();
    }

    @JsonProperty
    public Map<String, String> getPushDownDsl()
    {
        return pushDownDsl;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schema", schema)
                .add("table", table)
                .add("addresses", addresses)
                .add("searchRequest", searchRequest)
                .add("timeValue", timeValue)
                .add("pushDownDsl", pushDownDsl)
                .toString();
    }
}
