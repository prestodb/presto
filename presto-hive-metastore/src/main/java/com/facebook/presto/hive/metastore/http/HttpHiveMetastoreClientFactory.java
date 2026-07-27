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
package com.facebook.presto.hive.metastore.http;

import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.MetastoreClientFactory;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreClient;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.Timeout;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.http.HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.BASIC;
import static com.facebook.presto.hive.metastore.http.HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.BEARER;
import static com.facebook.presto.hive.metastore.http.HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.NONE;
import static com.facebook.presto.hive.metastore.util.HiveMetastoreUtils.buildSslContext;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HttpHiveMetastoreClientFactory
        implements MetastoreClientFactory
{
    private final HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType authType;
    private final Optional<SSLContext> sslContext;
    private final String catalogName;
    private final CloseableHttpClient httpClient;

    public HttpHiveMetastoreClientFactory(HttpHiveMetastoreConfig metastoreClientConfig, String catalogName)
    {
        requireNonNull(metastoreClientConfig, "metastoreClientConfig is null");
        int readTimeoutMillis = toIntExact(metastoreClientConfig.getHttpReadTimeout().toMillis());
        this.sslContext = requireNonNull(createSslContext(metastoreClientConfig), "sslContext is null");
        Map<String, String> httpAdditionalHeaders = requireNonNull(metastoreClientConfig.getHttpAdditionalHeaders(), "httpAdditionalHeaders is null");
        this.authType = requireNonNull(metastoreClientConfig.getHttpHiveMetastoreClientAuthenticationType(), "authType is null");
        Optional<String> httpBearerToken = authType == BEARER ? requireNonNull(metastoreClientConfig.getHttpBearerToken(), "httpBearerToken is null") : Optional.empty();
        Optional<String> httpMetastoreBasicUsername = authType == BASIC ? requireNonNull(metastoreClientConfig.getHttpMetastoreBasicUsername(), "httpMetastoreBasicUsername is null") : Optional.empty();
        Optional<String> httpMetastoreBasicPassword = authType == BASIC ? requireNonNull(metastoreClientConfig.getHttpMetastoreBasicPassword(), "httpMetastoreBasicPassword is null") : Optional.empty();
        this.catalogName = catalogName;

        RegistryBuilder<ConnectionSocketFactory> socketFactoryRegistryBuilder = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory());
        if (this.sslContext.isPresent()) {
            socketFactoryRegistryBuilder.register("https", new SSLConnectionSocketFactory(this.sslContext.get()));
        }
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistryBuilder.build());
        connectionManager.setMaxTotal(100);
        connectionManager.setDefaultMaxPerRoute(50);

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .setConnectionManager(connectionManager);

        if (authType == BEARER) {
            checkArgument(httpBearerToken.isPresent(),
                    "Bearer token must be provided when 'hive.metastore.http.client.authentication.type' is set to BEARER");
            httpClientBuilder.addRequestInterceptorFirst((HttpRequest request, EntityDetails entity, HttpContext context) -> {
                request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + httpBearerToken.get());
            });
        }
        else if (authType == BASIC) {
            checkArgument(httpMetastoreBasicUsername.isPresent() && httpMetastoreBasicPassword.isPresent(),
                    "Client username and password must be provided when 'hive.metastore.http.client.authentication.type' is set to BASIC");
            String auth = httpMetastoreBasicUsername.get() + ":" + httpMetastoreBasicPassword.get();
            String encodedAuth = getEncoder().encodeToString(auth.getBytes(UTF_8));
            httpClientBuilder.addRequestInterceptorFirst((HttpRequest request, EntityDetails entity, HttpContext context) -> {
                request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
            });
        }

        if (!httpAdditionalHeaders.isEmpty()) {
            httpClientBuilder.addRequestInterceptorFirst((HttpRequest request, EntityDetails entity, HttpContext context) -> {
                httpAdditionalHeaders.forEach(request::addHeader);
            });
        }

        RequestConfig requestConfig = RequestConfig.custom()
                .setResponseTimeout(Timeout.ofMilliseconds(readTimeoutMillis))
                .setConnectTimeout(Timeout.ofMilliseconds(readTimeoutMillis))
                .build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        this.httpClient = httpClientBuilder.build();
    }

    @Inject
    public HttpHiveMetastoreClientFactory(HttpHiveMetastoreConfig metastoreClientConfig, HiveCommonClientConfig hiveCommonClientConfig)
    {
        this(metastoreClientConfig, hiveCommonClientConfig.getCatalogName());
    }

    private static Optional<SSLContext> createSslContext(HttpHiveMetastoreConfig config)
    {
        return buildSslContext(config.getHttpMetastoreTlsEnabled(),
                Optional.ofNullable(config.getHttpMetastoreTlsKeystorePath()),
                Optional.ofNullable(config.getHttpMetastoreTlsKeystorePassword()),
                Optional.ofNullable(config.getHttpMetastoreTlsTruststorePath()),
                Optional.ofNullable(config.getHttpMetastoreTlsTruststorePassword()));
    }

    @Override
    public HiveMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        String scheme = uri.getScheme().toLowerCase(ENGLISH);
        if ("https".equals(scheme)) {
            if (!sslContext.isPresent()) {
                throw new IllegalStateException("HTTPS metastore requires TLS to be enabled and configured");
            }
        }
        else if ("http".equals(scheme)) {
            checkArgument(NONE.equals(authType),
                    "'hive.metastore.http.client.authentication.type' does not have a valid authentication type when using http metastore URIs in 'hive.metastore.uri'");
        }
        else {
            throw new IllegalArgumentException("Unsupported scheme for HTTP metastore client: " + scheme);
        }
        return new ThriftHiveMetastoreClient(new THttpClient(uri.toString(), httpClient), catalogName);
    }
}
