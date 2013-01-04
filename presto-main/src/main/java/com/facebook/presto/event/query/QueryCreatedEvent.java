package com.facebook.presto.event.query;

import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.net.URI;

@Immutable
@EventType("QueryCreated")
public class QueryCreatedEvent
{
    private final String queryId;
    private final URI uri;
    private final String query;
    private final DateTime createTime;

    public QueryCreatedEvent(
            String queryId,
            URI uri,
            String query,
            DateTime createTime)
    {
        this.queryId = queryId;
        this.uri = uri;
        this.query = query;
        this.createTime = createTime;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
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
