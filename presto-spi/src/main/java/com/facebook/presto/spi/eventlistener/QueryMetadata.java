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

package com.facebook.presto.spi.eventlistener;

import java.net.URI;

public class QueryMetadata
{
    private final String queryId;
    private final String transactionId;

    private final String query;
    private final String queryState;

    private final URI uri;

    private final String payload;

    public QueryMetadata(String queryId, String transactionId, String query, String queryState, URI uri, String payload)
    {
        this.queryId = queryId;
        this.transactionId = transactionId;
        this.query = query;
        this.queryState = queryState;
        this.uri = uri;
        this.payload = payload;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getTransactionId()
    {
        return transactionId;
    }

    public String getQuery()
    {
        return query;
    }

    public String getQueryState()
    {
        return queryState;
    }

    public URI getUri()
    {
        return uri;
    }

    public String getPayload()
    {
        return payload;
    }
}
