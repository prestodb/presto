
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final ElasticsearchTableSource uri;
    private final boolean remotelyAccessible;
    private final ImmutableList<HostAddress> addresses;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("uri") ElasticsearchTableSource uri)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.uri = checkNotNull(uri, "uri is null");

//        if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
        remotelyAccessible = true;
        //addresses = ImmutableList.of(HostAddress.fromUri(uri));
        addresses = ImmutableList.of();
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public ElasticsearchTableSource getUri()
    {
        return uri;
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
