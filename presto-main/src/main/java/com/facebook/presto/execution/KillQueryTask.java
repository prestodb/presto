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
package com.facebook.presto.execution;

import java.util.Map;
import java.util.Map.Entry;

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;

import com.facebook.presto.Session;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.server.PrestoServer;
import com.facebook.presto.sql.tree.KillQuery;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KillQueryTask
        implements DataDefinitionTask<KillQuery>
{
    private static final String USER_AGENT_VALUE = KillQueryTask.class.getSimpleName() +
            "/" +
            firstNonNull(KillQueryTask.class.getPackage().getImplementationVersion(), "unknown");

    @Override
    public String getName()
    {
        return "KILL QUERY";
    }

    @Override
    public void execute(KillQuery statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        ClientSession clientSession = session.toClientSession(PrestoServer.getDiscoveryUri(), false);
        Request request = buildKillRequest(clientSession, statement.getQueryID());
        HttpClient httpClient = new JettyHttpClient();
        JsonCodec<QueryResults> queryResultsCodec = jsonCodec(QueryResults.class);
        FullJsonResponseHandler<QueryResults> responseHandler = createFullJsonResponseHandler(queryResultsCodec);
        JsonResponse<QueryResults> response = httpClient.execute(request, responseHandler);

        if (response.getStatusCode() != HttpStatus.NO_CONTENT.code()) {
            throw requestFailedException("killing query failed", request, response);
        }
    }

    private static Request buildKillRequest(ClientSession session, String queryID)
    {
        Request.Builder builder = prepareDelete()
                .setUri(uriBuilderFrom(session.getServer()).replacePath("/v1/query/" + queryID).build())
                .setBodyGenerator(createStaticBodyGenerator(queryID, UTF_8));

        if (session.getUser() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_USER, session.getUser());
        }
        if (session.getSource() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
        }
        if (session.getCatalog() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        builder.setHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZoneId());
        builder.setHeader(PrestoHeaders.PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        builder.setHeader(USER_AGENT, USER_AGENT_VALUE);

        Map<String, String> property = session.getProperties();
        for (Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        return builder.build();
    }

    private RuntimeException requestFailedException(String task, Request request, JsonResponse<QueryResults> response)
    {
        if (!response.hasValue()) {
            return new RuntimeException(format("Error " + task + " at %s returned an invalid response: %s", request.getUri(), response), response.getException());
        }
        return new RuntimeException(format("Error " + task + " at %s returned %s: %s",
                request.getUri(),
                response.getStatusCode(),
                response.getStatusMessage()));
    }
}
