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
    private final String environment;
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
            String source,
            String environment,
            String catalog,
            String schema,
            String remoteClientAddress,
            String userAgent,
            URI uri,
            String query,
            DateTime createTime)
    {
        this.queryId = queryId;
        this.user = user;
        this.source = source;
        this.environment = environment;
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
    public String getEnvironment()
    {
        return environment;
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
