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
package com.facebook.presto.server.security.oauth2;

import com.facebook.airlift.http.server.HttpServerConfig;
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.UriBuilder;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.client.OkHttpUtil.setupInsecureSsl;
import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getFullRequestURL;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.net.HttpHeaders.HOST;
import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SimpleProxyServer
        implements Closeable
{
    private final TestingHttpServer server;

    public SimpleProxyServer(URI forwardBaseURI)
            throws Exception
    {
        server = createSimpleProxyServer(forwardBaseURI);
        server.start();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            server.stop();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public URI getHttpsBaseUrl()
    {
        return server.getHttpServerInfo().getHttpsUri();
    }

    private TestingHttpServer createSimpleProxyServer(URI forwardBaseURI)
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpPort(0)
                .setHttpsEnabled(true)
                .setHttpsPort(0)
                .setKeystorePath(Resources.getResource("cert/localhost.pem").getPath());
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        return new TestingHttpServer(httpServerInfo, nodeInfo, config, new SimpleProxy(forwardBaseURI), ImmutableMap.of(), ImmutableMap.of(), Optional.empty());
    }

    private class SimpleProxy
            extends HttpServlet
    {
        private final OkHttpClient httpClient;
        private final URI forwardBaseURI;

        private final Logger logger = Logger.get(SimpleProxy.class);

        public SimpleProxy(URI forwardBaseURI)
        {
            this.forwardBaseURI = forwardBaseURI;
            OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
            setupInsecureSsl(httpClientBuilder);
            httpClient = httpClientBuilder
                    .followRedirects(false)
                    .connectTimeout(100, SECONDS)
                    .writeTimeout(100, SECONDS)
                    .readTimeout(100, SECONDS)
                    .build();
        }

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse servletResponse)
                throws ServletException, IOException
        {
            UriBuilder requestUriBuilder = UriBuilder.fromUri(getFullRequestURL(request));
            requestUriBuilder
                    .scheme("http")
                    .host(forwardBaseURI.getHost())
                    .port(forwardBaseURI.getPort());

            String hostHeader = new StringBuilder().append(request.getRemoteHost()).append(":").append(request.getLocalPort()).toString();
            Cookie[] cookies = Optional.ofNullable(request.getCookies()).orElse(new Cookie[0]);
            String requestUri = requestUriBuilder.build().toString();
            Request.Builder reqBuilder = new Request.Builder()
                    .url(requestUri)
                    .addHeader(X_FORWARDED_PROTO, "https")
                    .addHeader(X_FORWARDED_FOR, request.getRemoteAddr())
                    .addHeader(HOST, hostHeader)
                    .get();

            if (cookies.length > 0) {
                for (Cookie cookie : cookies) {
                    reqBuilder.addHeader("Cookie", cookie.getName() + "=" + cookie.getValue());
                }
            }
            Response response;
            try {
                response = httpClient.newCall(reqBuilder.build()).execute();
                servletResponse.setStatus(response.code());

                Headers responseHeaders = response.headers();
                responseHeaders.names().stream().forEach(headerName -> {
                    // Headers can have multiple values
                    List<String> headerValues = responseHeaders.values(headerName);
                    headerValues.forEach(headerValue -> {
                        servletResponse.addHeader(headerName, headerValue);
                    });
                });

                //copy the response body to the servlet response.
                InputStream is = response.body().byteStream();
                OutputStream os = servletResponse.getOutputStream();
                byte[] buffer = new byte[10 * 1024];
                int read;
                while ((read = is.read(buffer)) != -1) {
                    os.write(buffer, 0, read);
                }
            }
            catch (Exception e) {
                logger.error(format("Encountered an error while proxying request to %s", requestUri), e);
                servletResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    // This is just to help iterate changes that might need to be made in the future to the Simple Proxy Server for test purposes.
    // Waiting for the tests to run can be really slow so having this helper here is nice if you want quicker feedback.
    private static void runTestServer()
            throws Exception
    {
        SimpleProxyServer test = new SimpleProxyServer(new URI(""));
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();
        setupInsecureSsl(httpClientBuilder);
        OkHttpClient client = httpClientBuilder.build();
        Request.Builder req = new Request.Builder().url(test.getHttpsBaseUrl().resolve("/v1/query").toString()).get();
        Logger logger = Logger.get("Run Test Server Debug Helper");
        try {
            client.newCall(req.build()).execute();
        }
        catch (Exception e) {
            logger.error(e);
        }

        test.close();
    }

    public static void main(String[] args)
            throws Exception
    {
        runTestServer();
    }
}
