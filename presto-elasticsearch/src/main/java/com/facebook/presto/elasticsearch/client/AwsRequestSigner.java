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
package com.facebook.presto.elasticsearch.client;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.HttpMethodName;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

class AwsRequestSigner
        implements HttpRequestInterceptor
{
    private static final String SERVICE_NAME = "es";
    private final AWSCredentialsProvider credentialsProvider;
    private final AWS4Signer signer;

    public AwsRequestSigner(String region, AWSCredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = credentialsProvider;
        this.signer = new AWS4Signer();

        signer.setServiceName(SERVICE_NAME);
        signer.setRegionName(region);
    }

    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws IOException
    {
        String method = request.getRequestLine().getMethod();

        URI uri = URI.create(request.getRequestLine().getUri());
        URIBuilder uriBuilder = new URIBuilder(uri);

        Map<String, List<String>> parameters = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        for (NameValuePair parameter : uriBuilder.getQueryParams()) {
            parameters.computeIfAbsent(parameter.getName(), key -> new ArrayList<>())
                    .add(parameter.getValue());
        }

        Map<String, String> headers = Arrays.stream(request.getAllHeaders())
                .collect(toImmutableMap(Header::getName, Header::getValue));

        InputStream content = null;
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
            if (enclosingRequest.getEntity() != null) {
                content = enclosingRequest.getEntity().getContent();
            }
        }

        DefaultRequest<?> awsRequest = new DefaultRequest<>(SERVICE_NAME);

        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host != null) {
            awsRequest.setEndpoint(URI.create(host.toURI()));
        }
        awsRequest.setHttpMethod(HttpMethodName.fromValue(method));
        awsRequest.setResourcePath(uri.getRawPath());
        awsRequest.setContent(content);
        awsRequest.setParameters(parameters);
        awsRequest.setHeaders(headers);

        signer.sign(awsRequest, credentialsProvider.getCredentials());

        Header[] newHeaders = awsRequest.getHeaders().entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);

        request.setHeaders(newHeaders);

        InputStream newContent = awsRequest.getContent();
        checkState(newContent == null || request instanceof HttpEntityEnclosingRequest);
        if (newContent != null) {
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(newContent);
            ((HttpEntityEnclosingRequest) request).setEntity(entity);
        }
    }
}
