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
package io.prestosql.jdbc;

import io.airlift.json.JsonCodec;
import io.prestosql.client.ClientException;
import io.prestosql.client.ClientSession;
import io.prestosql.client.JsonResponse;
import io.prestosql.client.ServerInfo;
import io.prestosql.client.StatementClient;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.net.URI;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.client.StatementClientFactory.newStatementClient;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class QueryExecutor
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);

    private final OkHttpClient httpClient;

    public QueryExecutor(OkHttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public StatementClient startQuery(ClientSession session, String query)
    {
        return newStatementClient(httpClient, session, query);
    }

    public ServerInfo getServerInfo(URI server)
    {
        HttpUrl url = HttpUrl.get(server);
        if (url == null) {
            throw new ClientException("Invalid server URL: " + server);
        }
        url = url.newBuilder().encodedPath("/v1/info").build();

        Request request = new Request.Builder().url(url).build();

        JsonResponse<ServerInfo> response = JsonResponse.execute(SERVER_INFO_CODEC, httpClient, request);
        if (!response.hasValue()) {
            throw new RuntimeException(format("Request to %s failed: %s [Error: %s]", server, response, response.getResponseBody()));
        }
        return response.getValue();
    }
}
