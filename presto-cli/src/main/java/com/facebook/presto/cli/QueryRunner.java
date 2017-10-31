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

import com.facebook.presto.client.ClientException;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.StatementClient;
import com.google.common.net.HostAndPort;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.ClientSession.stripTransactionId;
import static com.facebook.presto.client.OkHttpUtil.basicAuth;
import static com.facebook.presto.client.OkHttpUtil.setupHttpProxy;
import static com.facebook.presto.client.OkHttpUtil.setupKerberos;
import static com.facebook.presto.client.OkHttpUtil.setupSocksProxy;
import static com.facebook.presto.client.OkHttpUtil.setupSsl;
import static com.facebook.presto.client.OkHttpUtil.setupTimeouts;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryRunner
        implements Closeable
{
    private final AtomicReference<ClientSession> session;
    private final OkHttpClient httpClient;

    public QueryRunner(
            ClientSession session,
            Optional<HostAndPort> socksProxy,
            Optional<HostAndPort> httpProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> user,
            Optional<String> password,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosRemoteServiceName,
            Optional<String> kerberosConfigPath,
            Optional<String> kerberosKeytabPath,
            Optional<String> kerberosCredentialCachePath,
            boolean kerberosUseCanonicalHostname,
            boolean kerberosEnabled)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        setupTimeouts(builder, 5, SECONDS);
        setupSocksProxy(builder, socksProxy);
        setupHttpProxy(builder, httpProxy);
        setupSsl(builder, keystorePath, keystorePassword, truststorePath, truststorePassword);
        setupBasicAuth(builder, session, user, password);

        if (kerberosEnabled) {
            setupKerberos(
                    builder,
                    kerberosRemoteServiceName.orElseThrow(() -> new ClientException("Kerberos remote service name must be set")),
                    kerberosUseCanonicalHostname,
                    kerberosPrincipal,
                    kerberosConfigPath.map(File::new),
                    kerberosKeytabPath.map(File::new),
                    kerberosCredentialCachePath.map(File::new));
        }

        this.httpClient = builder.build();
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
        return new Query(startInternalQuery(session.get(), query));
    }

    public StatementClient startInternalQuery(String query)
    {
        return startInternalQuery(stripTransactionId(session.get()), query);
    }

    private StatementClient startInternalQuery(ClientSession session, String query)
    {
        return new StatementClient(httpClient, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private static void setupBasicAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> user,
            Optional<String> password)
    {
        if (user.isPresent() && password.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using username/password requires HTTPS to be enabled");
            clientBuilder.addInterceptor(basicAuth(user.get(), password.get()));
        }
    }
}
