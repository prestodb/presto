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
package com.facebook.presto.mcp;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.StatementClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Client for executing queries against a Presto server.
 * Handles authentication (Basic Auth or Bearer Token) and SSL configuration.
 */
public class PrestoQueryClient
{
    private final McpServerConfig mcpServerConfig;
    private final OkHttpClient httpClient = new OkHttpClient();

    @Inject
    public PrestoQueryClient(McpServerConfig mcpServerConfig)
    {
        this.mcpServerConfig = mcpServerConfig;
    }

    /**
     * Executes a SQL query against the configured Presto server.
     *
     * @param sql the SQL query to execute
     * @param token optional authentication token (overrides config if provided)
     * @return list of result rows, where each row is a list of column values
     */
    public List<List<Object>> runQuery(String sql, String token)
    {
        StatementClient client = newStatementClient(buildHttpClientWithAuth(token),
                createClientSession(mcpServerConfig.getPrestoUri()), sql);
        try {
            List<List<Object>> rows = new ArrayList<>();

            while (client.isRunning() && client.advance()) {
                if (client.currentData() != null && client.currentData().getData() != null) {
                    for (List<Object> row : client.currentData().getData()) {
                        rows.add(row);
                    }
                }
            }
            return rows;
        }
        finally {
            client.close();
        }
    }

    private OkHttpClient buildHttpClientWithAuth(String authorizationHeader)
    {
        String authHeader = authorizationHeader;

        if (authHeader == null || authHeader.isEmpty()) {
            if (mcpServerConfig.getPrestoPassword() != null && !mcpServerConfig.getPrestoPassword().isEmpty()) {
                authHeader = Credentials.basic(mcpServerConfig.getPrestoUser(), mcpServerConfig.getPrestoPassword());
            }
            else if (mcpServerConfig.getPrestoToken() != null && !mcpServerConfig.getPrestoToken().isEmpty()) {
                authHeader = "Bearer " + mcpServerConfig.getPrestoToken();
            }
        }

        OkHttpClient.Builder builder = httpClient.newBuilder();

        if (mcpServerConfig.isSslVerificationDisabled()) {
            builder = configureInsecureSsl(builder);
        }

        if (authHeader == null || authHeader.isEmpty()) {
            return builder.build(); // no auth
        }

        final String finalAuthHeader = authHeader;

        return builder
                .addInterceptor(chain -> {
                    Request original = chain.request();
                    Request.Builder reqBuilder = original.newBuilder()
                            .header("Authorization", finalAuthHeader)
                            .method(original.method(), original.body());
                    return chain.proceed(reqBuilder.build());
                })
                .build();
    }

    /**
     * Configures the HTTP client to accept all SSL certificates without verification.
     * WARNING: This should only be used for development/testing with self-signed certificates.
     * Do not use in production as it disables SSL security.
     *
     * @param builder the OkHttpClient builder to configure
     * @return the configured builder with SSL verification disabled
     */
    private OkHttpClient.Builder configureInsecureSsl(OkHttpClient.Builder builder)
    {
        try {
            TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager()
                {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {}

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {}

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers()
                    {
                        return new java.security.cert.X509Certificate[]{};
                    }
                }
            };

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            return builder
                    .sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0])
                    .hostnameVerifier((hostname, session) -> true);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to configure insecure SSL", e);
        }
    }

    private ClientSession createClientSession(URI prestoUri)
    {
        return new ClientSession(
                prestoUri,
                mcpServerConfig.getPrestoUser(),
                "source",
                Optional.empty(),
                ImmutableSet.of(),
                null,
                null,
                null,
                "America/Los_Angeles",
                Locale.ENGLISH,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                null,
                new Duration(2, MINUTES),
                true,
                ImmutableMap.of(),
                ImmutableMap.of(),
                false);
    }

    /**
     * Applies a default LIMIT clause to queries that don't already have one.
     * This prevents accidentally returning huge result sets.
     *
     * @param sql the SQL query
     * @return the SQL query with LIMIT clause added if not present
     */
    public String applyLimit(String sql)
    {
        String lower = sql.toLowerCase();
        if (!lower.contains("limit ")) {
            return sql + " LIMIT " + mcpServerConfig.getDefaultLimit();
        }
        return sql;
    }
}
