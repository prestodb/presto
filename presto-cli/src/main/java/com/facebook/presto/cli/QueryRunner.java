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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Objects;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.http.client.HttpStatus.Family;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

public class QueryRunner
        implements Closeable
{
    private static final String USER_AGENT_VALUE = QueryRunner.class.getSimpleName() +
    "/" +
    Objects.firstNonNull(QueryRunner.class.getPackage().getImplementationVersion(), "unknown");

    private final JsonCodec<QueryResults> queryResultsCodec;
    private final AtomicReference<ClientSession> session;
    private final HttpClient httpClient;

    public QueryRunner(ClientSession session, JsonCodec<QueryResults> queryResultsCodec)
    {
        this.session = new AtomicReference<>(checkNotNull(session, "session is null"));
        this.queryResultsCodec = checkNotNull(queryResultsCodec, "queryResultsCodec is null");
        this.httpClient = new JettyHttpClient(new HttpClientConfig().setConnectTimeout(new Duration(10, TimeUnit.SECONDS)));
    }

    public ClientSession getSession()
    {
        return session.get();
    }

    public void setSession(ClientSession session)
    {
        this.session.set(checkNotNull(session, "session is null"));
    }

    public Query startQuery(String query)
    {
        return new Query(startInternalQuery(query));
    }

    public StatementClient startInternalQuery(String query)
    {
        return new StatementClient(httpClient, queryResultsCodec, session.get(), query);
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    public static QueryRunner create(ClientSession session)
    {
        return new QueryRunner(session, jsonCodec(QueryResults.class));
    }

    public boolean removePrestoServerSession()
    {
        String sessionId = session.get().getSessionId();
        if (sessionId != null) {
            String queryPath = "v1/statement/sessions/" + sessionId;
            Request request = prepareDelete()
                .setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(uriBuilderFrom(session.get().getServer()).replacePath(queryPath).build())
                .build();
            StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
            return familyForStatusCode(status.getStatusCode()) == Family.SUCCESSFUL;
        }
        return true;
    }
}
