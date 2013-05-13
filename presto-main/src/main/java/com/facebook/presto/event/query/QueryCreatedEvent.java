package com.facebook.presto.event.query;

import com.facebook.presto.execution.QueryId;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.net.URI;

@Immutable
@EventType("QueryCreated")
public class QueryCreatedEvent
{
    private final QueryId queryId;
    private final String user;
    private final String source;
    private final String catalog;
    private final String schema;
    private final String remoteClientAddress;
    private final String userAgent;
    private final URI uri;
    private final String query;
    private final DateTime createTime;

    public QueryCreatedEvent(
            QueryId queryId,
            String user,
            String source, String catalog,
            String schema,
            String remoteClientAddress, String userAgent, URI uri,
            String query,
            DateTime createTime)
    {
        this.queryId = queryId;
        this.user = user;
        this.source = source;
        this.catalog = catalog;
        this.schema = schema;
        this.remoteClientAddress = remoteClientAddress;
        this.userAgent = userAgent;
        this.uri = uri;
        this.query = query;
        this.createTime = createTime;
    }

    @EventField
    public String getQueryId()
    {
        return queryId.toString();
    }

    @EventField
    public String getUser()
    {
        return user;
    }

    @EventField
    public String getSource()
    {
        return source;
    }

    @EventField
    public String getCatalog()
    {
        return catalog;
    }

    @EventField
    public String getSchema()
    {
        return schema;
    }

    @EventField
    public String getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    @EventField
    public String getUserAgent()
    {
        return userAgent;
    }

    @EventField
    public String getUri()
    {
        return uri.toString();
    }

    @EventField
    public String getQuery()
    {
        return query;
    }

    @EventField
    public DateTime getCreateTime()
    {
        return createTime;
    }
}
