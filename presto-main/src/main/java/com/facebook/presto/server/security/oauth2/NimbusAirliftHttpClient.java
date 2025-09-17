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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.ResponseHandlerUtils;
import com.facebook.airlift.http.client.StringResponseHandler;
import com.google.common.collect.ImmutableMultimap;
import com.nimbusds.jose.util.Resource;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import jakarta.ws.rs.core.UriBuilder;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.nimbusds.oauth2.sdk.http.HTTPRequest.Method.DELETE;
import static com.nimbusds.oauth2.sdk.http.HTTPRequest.Method.GET;
import static com.nimbusds.oauth2.sdk.http.HTTPRequest.Method.POST;
import static com.nimbusds.oauth2.sdk.http.HTTPRequest.Method.PUT;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class NimbusAirliftHttpClient
        implements NimbusHttpClient
{
    private final HttpClient httpClient;

    @Inject
    public NimbusAirliftHttpClient(@ForOAuth2 HttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public Resource retrieveResource(URL url)
            throws IOException
    {
        try {
            StringResponseHandler.StringResponse response = httpClient.execute(
                    prepareGet().setUri(url.toURI()).build(),
                    createStringResponseHandler());
            return new Resource(response.getBody(), response.getHeader(CONTENT_TYPE));
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T execute(com.nimbusds.oauth2.sdk.Request nimbusRequest, Parser<T> parser)
    {
        HTTPRequest httpRequest = nimbusRequest.toHTTPRequest();
        HTTPRequest.Method method = httpRequest.getMethod();

        Request.Builder request = new Request.Builder()
                .setMethod(method.name())
                .setFollowRedirects(httpRequest.getFollowRedirects());

        UriBuilder url = UriBuilder.fromUri(httpRequest.getURI());
        if (method.equals(GET) || method.equals(DELETE)) {
            httpRequest.getQueryParameters().forEach((key, value) -> url.queryParam(key, value.toArray()));
        }

        url.fragment(httpRequest.getFragment());

        request.setUri(url.build());

        ImmutableMultimap.Builder<String, String> headers = ImmutableMultimap.builder();
        httpRequest.getHeaderMap().forEach(headers::putAll);
        request.addHeaders(headers.build());

        if (method.equals(POST) || method.equals(PUT)) {
            String query = httpRequest.getQuery();
            if (query != null) {
                request.setBodyGenerator(createStaticBodyGenerator(httpRequest.getQuery(), UTF_8));
            }
        }
        return httpClient.execute(request.build(), new NimbusResponseHandler<>(parser));
    }

    public static class NimbusResponseHandler<T>
            implements ResponseHandler<T, RuntimeException>
    {
        private final StringResponseHandler handler = createStringResponseHandler();
        private final Parser<T> parser;

        public NimbusResponseHandler(Parser<T> parser)
        {
            this.parser = requireNonNull(parser, "parser is null");
        }

        @Override
        public T handleException(Request request, Exception exception)
        {
            throw ResponseHandlerUtils.propagate(request, exception);
        }

        @Override
        public T handle(Request request, Response response)
        {
            StringResponseHandler.StringResponse stringResponse = handler.handle(request, response);
            HTTPResponse nimbusResponse = new HTTPResponse(response.getStatusCode());
            response.getHeaders().asMap().forEach((name, values) -> nimbusResponse.setHeader(name.toString(), values.toArray(new String[0])));
            nimbusResponse.setContent(stringResponse.getBody());
            try {
                return parser.parse(nimbusResponse);
            }
            catch (ParseException e) {
                throw new RuntimeException(format("Unable to parse response status=[%d], body=[%s]", stringResponse.getStatusCode(), stringResponse.getBody()), e);
            }
        }
    }
}
