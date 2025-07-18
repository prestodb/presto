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
import com.facebook.presto.execution.TaskManagerConfig;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
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
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.security.pem.PemReader.loadPrivateKey;
import static com.facebook.airlift.security.pem.PemReader.readCertificateChain;
import static io.netty.handler.ssl.ApplicationProtocolConfig.Protocol.ALPN;
import static io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT;
import static io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_1_1;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_2;

public class ReactorNettyHttpClient
        implements com.facebook.airlift.http.client.HttpClient, Closeable
{
    private static final Logger log = Logger.get(ReactorNettyHttpClient.class);
    private static final HeaderName CONTENT_TYPE_HEADER_NAME = HeaderName.of("Content-Type");
    private static final HeaderName CONTENT_LENGTH_HEADER_NAME = HeaderName.of("Content-Length");

    private final HttpClient httpClient;

    @Inject
    public ReactorNettyHttpClient(TaskManagerConfig config)
    {
        SslContext sslContext;
        try {
            File keyFile = new File("/var/facebook/x509_identities/server.pem");
            File trustCertificateFile = new File("/var/facebook/rootcanal/ca.pem");
            List<String> ciphers = Arrays.asList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_256_GCM_SHA384");
            PrivateKey privateKey = loadPrivateKey(keyFile, Optional.of("password"));
            X509Certificate[] certificateChain = readCertificateChain(keyFile).toArray(new X509Certificate[0]);
            X509Certificate[] trustChain = readCertificateChain(trustCertificateFile).toArray(new X509Certificate[0]);

            SslProvider provider = SslProvider.isAlpnSupported(SslProvider.OPENSSL) ? SslProvider.OPENSSL : SslProvider.JDK;
            sslContext = SslContextBuilder.forClient()
                    .sslProvider(provider)
                    .protocols("TLSv1.2")
                    .ciphers(ciphers)
                    .keyManager(privateKey, certificateChain)
                    .trustManager(trustChain)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(ALPN, NO_ADVERTISE, ACCEPT, HTTP_2, HTTP_1_1))
                    .build();
        }
        catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Failed to configure SSL context", e);
        }

        // Create base connection provider with limits
        ConnectionProvider pool = ConnectionProvider.builder("http2-pool")
                .allocationStrategy((Http2AllocationStrategy.builder()
                        .maxConnections(config.getNettyMaxTotalConnections())
                        .maxConcurrentStreams(config.getNettyMaxStreamPerChannel())
                        .minConnections(100).build()))
                .build();

        LoopResources loopResources = LoopResources.create("event-loop", Runtime.getRuntime().availableProcessors(), config.getNettyEventLoopThreadCount(), true, false);

        // Create HTTP/2 client
        SslContext finalSslContext = sslContext;
        this.httpClient = HttpClient.create(pool)
                .protocol(HttpProtocol.H2, HttpProtocol.HTTP11)
                .runOn(loopResources, true)
                .http2Settings(settings -> settings.initialWindowSize(65535).maxConcurrentStreams(config.getNettyMaxStreamPerChannel()).maxFrameSize(1048576))
                .secure(spec -> spec.sslContext(finalSslContext));
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
        switch (airliftRequest.getMethod()) {
            case "GET":
                client.get()
                        .uri(uri)
                        .responseSingle((response, bytes) -> bytes.asInputStream().zipWith(Mono.just(response)))
                        .subscribe(t -> onSuccess(responseHandler, t.getT1(), t.getT2(), listenableFuture), e -> onError(listenableFuture, e), () -> onComplete(listenableFuture));
                break;
            case "POST":
                byte[] postBytes = ((StaticBodyGenerator) airliftRequest.getBodyGenerator()).getBody();
                client.post()
                        .uri(uri)
                        .send(ByteBufFlux.fromInbound(Mono.just(postBytes)))
                        .responseSingle((response, bytes) -> bytes.asInputStream().zipWith(Mono.just(response)))
                        .subscribe(t -> onSuccess(responseHandler, t.getT1(), t.getT2(), listenableFuture), e -> onError(listenableFuture, e), () -> onComplete(listenableFuture));
                break;
            case "DELETE":
                client.delete()
                        .uri(uri)
                        .responseSingle((response, bytes) -> bytes.asInputStream().zipWith(Mono.just(response)))
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
            if (name.equalsIgnoreCase("content-length")) {
                String val = headers.get(name);
                contentLength = Integer.parseInt(val);
                responseHeaders.put(CONTENT_LENGTH_HEADER_NAME, val);
            }
            else if (name.equalsIgnoreCase("Content-Type")) {
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
