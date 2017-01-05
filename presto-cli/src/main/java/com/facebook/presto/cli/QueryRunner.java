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
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.spnego.KerberosConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class QueryRunner
        implements Closeable
{
    private final JsonCodec<QueryResults> queryResultsCodec;
    private final AtomicReference<ClientSession> session;
    private final HttpClient httpClient;

    public QueryRunner(
            ClientSession session,
            JsonCodec<QueryResults> queryResultsCodec,
            Optional<HostAndPort> socksProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosRemoteServiceName,
            boolean authenticationEnabled,
            KerberosConfig kerberosConfig)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.queryResultsCodec = requireNonNull(queryResultsCodec, "queryResultsCodec is null");
        this.httpClient = new JettyHttpClient(
                getHttpClientConfig(
                        socksProxy,
                        keystorePath,
                        keystorePassword,
                        truststorePath,
                        truststorePassword,
                        kerberosPrincipal,
                        kerberosRemoteServiceName,
                        authenticationEnabled),
                kerberosConfig,
                Optional.empty(),
                ImmutableList.of());
    }

    public ClientSession getSession()
    {
        return session.get();
    }

    public void setSession(ClientSession session)
    {
        this.session.set(requireNonNull(session, "session is null"));
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

    public static QueryRunner create(
            ClientSession session,
            Optional<HostAndPort> socksProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosRemoteServiceName,
            boolean authenticationEnabled,
            KerberosConfig kerberosConfig)
    {
        return new QueryRunner(
                session,
                jsonCodec(QueryResults.class),
                socksProxy,
                keystorePath,
                keystorePassword,
                truststorePath,
                truststorePassword,
                kerberosPrincipal,
                kerberosRemoteServiceName,
                authenticationEnabled,
                kerberosConfig);
    }

    private static HttpClientConfig getHttpClientConfig(
            Optional<HostAndPort> socksProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosRemoteServiceName,
            boolean authenticationEnabled)
    {
        HttpClientConfig httpClientConfig = new HttpClientConfig()
                .setConnectTimeout(new Duration(5, TimeUnit.SECONDS))
                .setRequestTimeout(new Duration(5, TimeUnit.SECONDS));

        socksProxy.ifPresent(httpClientConfig::setSocksProxy);

        httpClientConfig.setAuthenticationEnabled(authenticationEnabled);

        keystorePath.ifPresent(httpClientConfig::setKeyStorePath);
        keystorePassword.ifPresent(httpClientConfig::setKeyStorePassword);
        truststorePath.ifPresent(httpClientConfig::setTrustStorePath);
        truststorePassword.ifPresent(httpClientConfig::setTrustStorePassword);
        kerberosPrincipal.ifPresent(httpClientConfig::setKerberosPrincipal);
        kerberosRemoteServiceName.ifPresent(httpClientConfig::setKerberosRemoteServiceName);

        return httpClientConfig;
    }
}
