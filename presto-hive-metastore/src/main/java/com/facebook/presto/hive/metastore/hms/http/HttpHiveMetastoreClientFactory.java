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
package com.facebook.presto.hive.metastore.hms.http;

import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.metastore.hms.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.hms.MetastoreClientFactory;
import com.facebook.presto.hive.metastore.hms.ThriftHiveMetastoreClient;
import com.google.inject.Inject;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.BASIC;
import static com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.BEARER;
import static com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.NONE;
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
    private final int readTimeoutMillis;
    private final Optional<SSLContext> sslContext;
    private final Optional<String> httpBearerToken;
    private final Optional<String> httpMetastoreBasicUsername;
    private final Optional<String> httpMetastoreBasicPassword;
    private final Map<String, String> httpAdditionalHeaders;
    private final String httpUserName = "x-actor-username";
    private final String catalogName;

    @Inject
    public HttpHiveMetastoreClientFactory(HttpHiveMetastoreConfig httpHiveMetastoreConfig, HiveCommonClientConfig hiveCommonClientConfig)
    {
        requireNonNull(httpHiveMetastoreConfig, "HttpHiveMetastoreConfig is null");
        this.readTimeoutMillis = toIntExact(httpHiveMetastoreConfig.getHttpReadTimeout().toMillis());
        this.sslContext = requireNonNull(createSslContext(httpHiveMetastoreConfig), "sslContext is null");
        this.httpAdditionalHeaders = requireNonNull(httpHiveMetastoreConfig.getHttpAdditionalHeaders(), "httpAdditionalHeaders is null");
        this.authType = requireNonNull(httpHiveMetastoreConfig.getHttpHiveMetastoreClientAuthenticationType(), "authType is null");
        this.httpBearerToken = authType == BEARER ? requireNonNull(httpHiveMetastoreConfig.getHttpBearerToken(), "httpBearerToken is null") : Optional.empty();
        this.httpMetastoreBasicUsername = authType == BASIC ? requireNonNull(httpHiveMetastoreConfig.getHttpMetastoreBasicUsername(), "httpMetastoreBasicUsername is null") : Optional.empty();
        this.httpMetastoreBasicPassword = authType == BASIC ? requireNonNull(httpHiveMetastoreConfig.getHttpMetastoreBasicPassword(), "httpMetastoreBasicPassword is null") : Optional.empty();
        this.catalogName = hiveCommonClientConfig.getCatalogName();
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
        return new ThriftHiveMetastoreClient(createHttpTransport(uri), catalogName);
    }

    private TTransport createHttpTransport(URI uri) throws TTransportException
    {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        if ("https".equals(uri.getScheme().toLowerCase(ENGLISH))) {
            HostnameVerifier customHostnameVerifier = (hostname, session) -> true;
            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext.orElseThrow(null), customHostnameVerifier);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", sslSocketFactory)
                    .build();
            httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(socketFactoryRegistry));
            httpClientBuilder.addInterceptorFirst((HttpRequest request, HttpContext context) -> {
                switch (authType) {
                    case BEARER:
                        checkArgument(httpBearerToken.isPresent(),
                                "Bearer token must be provided when 'hive.metastore.http.client.authentication.type' is set to BEARER");
                        request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + httpBearerToken.get());
                        break;

                    case BASIC:
                        checkArgument(httpMetastoreBasicUsername.isPresent() && httpMetastoreBasicPassword.isPresent(),
                                "Client username and password must be provided when 'hive.metastore.http.client.authentication.type' is set to BASIC");
                        String auth = httpMetastoreBasicUsername.get() + ":" + httpMetastoreBasicPassword.get();
                        String encodedAuth = getEncoder().encodeToString(auth.getBytes(UTF_8));
                        request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
                        break;

                    default:
                        throw new IllegalArgumentException("'hive.metastore.http.client.authentication.type' must be set to either BASIC or BEARER when using HTTPS metastore URIs in 'hive.metastore.uri'");
                }
            });
        }
        else {
            checkArgument(NONE.equals(authType),
                    "'hive.metastore.http.client.authentication.type' does not have a valid authentication type when using http metastore URIs in 'hive.metastore.uri'");
            httpClientBuilder.addInterceptorFirst((HttpRequest request, HttpContext context) -> httpMetastoreBasicUsername.ifPresent(username -> request.addHeader(httpUserName, username)));
        }
        httpClientBuilder.addInterceptorFirst((HttpRequest request, HttpContext context) -> {
            httpAdditionalHeaders.forEach(request::addHeader);
        });

        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(readTimeoutMillis)
                .setConnectTimeout(readTimeoutMillis)
                .build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        return new THttpClient(uri.toString(), httpClientBuilder.build());
    }
}
