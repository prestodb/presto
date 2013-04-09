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
    private final String catalog;
    private final String schema;
    private final URI uri;
    private final String query;
    private final DateTime createTime;

    public QueryCreatedEvent(
            QueryId queryId,
            String user,
            String catalog,
            String schema,
            URI uri,
            String query,
            DateTime createTime)
    {
        this.queryId = queryId;
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
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
