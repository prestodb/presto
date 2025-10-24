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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.RequestStats;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StaticBodyGenerator;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.Http2AllocationStrategy;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.facebook.airlift.security.pem.PemReader.loadPrivateKey;
import static com.facebook.airlift.security.pem.PemReader.readCertificateChain;
import static io.netty.handler.ssl.ApplicationProtocolConfig.Protocol.ALPN;
import static io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT;
import static io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_1_1;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_2;
import static io.netty.handler.ssl.SslProtocols.TLS_v1_2;
import static io.netty.handler.ssl.SslProtocols.TLS_v1_3;
import static io.netty.handler.ssl.SslProvider.JDK;
import static io.netty.handler.ssl.SslProvider.OPENSSL;
import static io.netty.handler.ssl.SslProvider.isAlpnSupported;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;

public class ReactorNettyHttpClient
        implements com.facebook.airlift.http.client.HttpClient, Closeable
{
    private static final Logger log = Logger.get(ReactorNettyHttpClient.class);
    private static final HeaderName CONTENT_TYPE_HEADER_NAME = HeaderName.of("Content-Type");
    private static final HeaderName CONTENT_LENGTH_HEADER_NAME = HeaderName.of("Content-Length");

    private final Duration requestTimeout;
    private HttpClient httpClient;
    private final HttpClientConnectionPoolStats connectionPoolStats;
    private final HttpClientStats httpClientStats;

    @Inject
    public ReactorNettyHttpClient(ReactorNettyHttpClientConfig config, HttpClientConnectionPoolStats connectionPoolStats, HttpClientStats httpClientStats)
    {
        this.connectionPoolStats = connectionPoolStats;
        this.httpClientStats = httpClientStats;
        SslContext sslContext = null;
        if (config.isHttpsEnabled()) {
            try {
                File keyFile = new File(config.getKeyStorePath());
                File trustCertificateFile = new File(config.getTrustStorePath());
                if (!Files.exists(keyFile.toPath()) || !Files.isReadable(keyFile.toPath())) {
                    throw new IllegalArgumentException("KeyStore file path is unreadable or doesn't exist");
                }
                if (!Files.exists(trustCertificateFile.toPath()) || !Files.isReadable(trustCertificateFile.toPath())) {
                    throw new IllegalArgumentException("TrustStore file path is unreadable or doesn't exist");
                }
                PrivateKey privateKey = loadPrivateKey(keyFile, Optional.of(config.getKeyStorePassword()));
                X509Certificate[] certificateChain = readCertificateChain(keyFile).toArray(new X509Certificate[0]);
                X509Certificate[] trustChain = readCertificateChain(trustCertificateFile).toArray(new X509Certificate[0]);

                String os = System.getProperty("os.name");
                if (os.toLowerCase(Locale.ENGLISH).contains("linux")) {
                    // Make sure Open ssl is available for linux deployments
                    if (!OpenSsl.isAvailable()) {
                        throw new UnsupportedOperationException(format("OpenSsl is not unavailable. Stacktrace: %s", Arrays.toString(OpenSsl.unavailabilityCause().getStackTrace()).replace(',', '\n')));
                    }
                    // Make sure epoll threads are used for linux deployments
                    if (!Epoll.isAvailable()) {
                        throw new UnsupportedOperationException(format("Epoll is not unavailable. Stacktrace: %s", Arrays.toString(Epoll.unavailabilityCause().getStackTrace()).replace(',', '\n')));
                    }
                }

                SslProvider provider = isAlpnSupported(OPENSSL) ? OPENSSL : JDK;
                SslContextBuilder sslContextBuilder = SslContextBuilder.forClient()
                        .sslProvider(provider)
                        .protocols(TLS_v1_3, TLS_v1_2)
                        .keyManager(privateKey, certificateChain)
                        .trustManager(trustChain)
                        .applicationProtocolConfig(new ApplicationProtocolConfig(ALPN, NO_ADVERTISE, ACCEPT, HTTP_2, HTTP_1_1));
                if (config.getCipherSuites().isPresent()) {
                    sslContextBuilder.ciphers(Splitter
                            .on(',')
                            .trimResults()
                            .omitEmptyStrings()
                            .splitToList(config.getCipherSuites().get()));
                }

                sslContext = sslContextBuilder.build();
            }
            catch (IOException | GeneralSecurityException e) {
                throw new RuntimeException("Failed to configure SSL context", e);
            }
        }

        /*
         * This is like wrapper and underlying there is a separate pool of connections for http1 and http2 protocols. Basically different pools for different protocols.
         * Reactor Netty's HttpConnectionProvider will wrap this connection provider and handle protocol routing in the acquire() call. It examines
         * the configured protocols and routes requests appropriately. So the http2 allocation strategy defined here will only be used for http2 connections.
         */
        ConnectionProvider pool = ConnectionProvider.builder("shared-pool")
                .maxConnections(config.getMaxConnections())
                .fifo()
                .maxIdleTime(java.time.Duration.of(config.getMaxIdleTime().toMillis(), MILLIS))
                .evictInBackground(java.time.Duration.of(config.getEvictBackgroundTime().toMillis(), MILLIS))
                .pendingAcquireTimeout(java.time.Duration.of(config.getPendingAcquireTimeout().toMillis(), MILLIS))
                .metrics(true, () -> connectionPoolStats)
                .allocationStrategy((Http2AllocationStrategy.builder()
                        .maxConnections(config.getMaxConnections())
                        .maxConcurrentStreams(config.getMaxStreamPerChannel())
                        .minConnections(config.getMinConnections()).build()))
                .build();

        LoopResources loopResources = LoopResources.create("event-loop", config.getSelectorThreadCount(), config.getEventLoopThreadCount(), true, false);

        // Create HTTP/2 client
        SslContext finalSslContext = sslContext;
        this.httpClient = HttpClient
                // The custom pool is wrapped with a HttpConnectionProvider over here
                .create(pool)
                .protocol(HttpProtocol.H2, HttpProtocol.HTTP11)
                .runOn(loopResources, true)
                .http2Settings(settings -> {
                    settings.maxConcurrentStreams(config.getMaxStreamPerChannel());
                    settings.initialWindowSize((int) (config.getMaxInitialWindowSize().toBytes()));
                    settings.maxFrameSize((int) (config.getMaxFrameSize().toBytes()));
                })
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) config.getConnectTimeout().getValue())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                // Track HTTP client metrics
                .metrics(true, () -> httpClientStats, Function.identity());

        if (config.isHttpsEnabled()) {
            if (finalSslContext == null) {
                throw new IllegalStateException("SSL context must be configured for HTTPS");
            }
            httpClient = httpClient.secure(spec -> spec.sslContext(finalSslContext));
        }

        this.requestTimeout = config.getRequestTimeout();
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        throw new UnsupportedOperationException();
    }

    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request airliftRequest, ResponseHandler<T, E> responseHandler)
    {
        SettableFuture<Object> listenableFuture = SettableFuture.create();

        // Set the request headers
        HttpClient client = this.httpClient.headers(hdr -> {
            for (Map.Entry<String, String> entry : airliftRequest.getHeaders().entries()) {
                hdr.set(entry.getKey(), entry.getValue());
            }
        });

        URI uri = airliftRequest.getUri();
        Disposable disposable;
        switch (airliftRequest.getMethod()) {
            case "GET":
                disposable = client.get()
                        .uri(uri)
                        .responseSingle((response, bytes) -> bytes.asInputStream().zipWith(Mono.just(response)))
                        // Request timeout
                        .timeout(java.time.Duration.of(requestTimeout.toMillis(), MILLIS))
                        .subscribe(t -> onSuccess(responseHandler, t.getT1(), t.getT2(), listenableFuture), e -> onError(listenableFuture, e), () -> onComplete(listenableFuture));
                break;
            case "POST":
                byte[] postBytes = ((StaticBodyGenerator) airliftRequest.getBodyGenerator()).getBody();
                disposable = client.post()
                        .uri(uri)
                        .send(ByteBufFlux.fromInbound(Mono.just(postBytes)))
                        .responseSingle((response, bytes) -> bytes.asInputStream().zipWith(Mono.just(response)))
                        // Request timeout
                        .timeout(java.time.Duration.of(requestTimeout.toMillis(), MILLIS))
                        .subscribe(t -> onSuccess(responseHandler, t.getT1(), t.getT2(), listenableFuture), e -> onError(listenableFuture, e), () -> onComplete(listenableFuture));
                break;
            case "DELETE":
                disposable = client.delete()
                        .uri(uri)
                        .responseSingle((response, bytes) -> bytes.asInputStream().zipWith(Mono.just(response)))
                        // Request timeout
                        .timeout(java.time.Duration.of(requestTimeout.toMillis(), MILLIS))
                        .subscribe(t -> onSuccess(responseHandler, t.getT1(), t.getT2(), listenableFuture), e -> onError(listenableFuture, e), () -> onComplete(listenableFuture));
                break;
            default:
                throw new UnsupportedOperationException("Unexpected request: " + airliftRequest);
        }

        return new HttpResponseFuture()
        {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                disposable.dispose();
                return listenableFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled()
            {
                return listenableFuture.isCancelled();
            }

            @Override
            public boolean isDone()
            {
                return listenableFuture.isDone();
            }

            @Override
            public Object get()
                    throws InterruptedException, ExecutionException
            {
                return listenableFuture.get();
            }

            @Override
            public Object get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException
            {
                return listenableFuture.get(timeout, unit);
            }

            @Override
            public void addListener(Runnable listener, Executor executor)
            {
                listenableFuture.addListener(listener, executor);
            }

            @Override
            public String getState()
            {
                return "";
            }
        };
    }

    public void onSuccess(ResponseHandler responseHandler, InputStream inputStream, HttpClientResponse response, SettableFuture<Object> listenableFuture)
    {
        ListMultimap<HeaderName, String> responseHeaders = ArrayListMultimap.create();
        HttpHeaders headers = response.responseHeaders();
        int status = response.status().code();
        if (status != 200 && status != 204) {
            listenableFuture.setException(new RuntimeException("Invalid response status: " + status));
            return;
        }

        long contentLength = 0;
        // Iterate over the headers
        for (String name : headers.names()) {
            if (name.equalsIgnoreCase(CONTENT_LENGTH_HEADER_NAME.toString())) {
                String val = headers.get(name);
                contentLength = Integer.parseInt(val);
                responseHeaders.put(CONTENT_LENGTH_HEADER_NAME, val);
            }
            else if (name.equalsIgnoreCase(CONTENT_TYPE_HEADER_NAME.toString())) {
                responseHeaders.put(CONTENT_TYPE_HEADER_NAME, headers.get(name));
            }
            else {
                responseHeaders.put(HeaderName.of(name), headers.get(name));
            }
        }

        if (!responseHeaders.containsKey(CONTENT_TYPE_HEADER_NAME) || responseHeaders.get(CONTENT_TYPE_HEADER_NAME).size() != 1) {
            listenableFuture.setException(new RuntimeException("Expected ContentType header: " + responseHeaders));
            return;
        }

        try {
            long finalContentLength = contentLength;
            Object a = responseHandler.handle(null, new Response()
            {
                @Override
                public int getStatusCode()
                {
                    return status;
                }

                @Override
                public ListMultimap<HeaderName, String> getHeaders()
                {
                    return responseHeaders;
                }

                @Override
                public long getBytesRead()
                {
                    return finalContentLength;
                }

                @Override
                public InputStream getInputStream()
                        throws IOException
                {
                    return inputStream;
                }
            });
            // closing it here to prevent memory leak of bytebuf
            inputStream.close();
            listenableFuture.set(a);
        }
        catch (Exception e) {
            listenableFuture.setException(e);
        }
        finally {
            try {
                inputStream.close();
            }
            catch (IOException e) {
                log.warn(e, "Failed to close input stream");
            }
        }
    }

    public void onError(SettableFuture<Object> listenableFuture, Throwable t)
    {
        listenableFuture.setException(t);
    }

    public void onComplete(SettableFuture<Object> listenableFuture)
    {
        if (!listenableFuture.isDone()) {
            listenableFuture.setException(new RuntimeException("completed without success or failure"));
        }
    }

    @Override
    public RequestStats getStats()
    {
        return null;
    }

    @Override
    public long getMaxContentLength()
    {
        return 0;
    }

    @Override
    public void close()
    {
        // void
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }
}
