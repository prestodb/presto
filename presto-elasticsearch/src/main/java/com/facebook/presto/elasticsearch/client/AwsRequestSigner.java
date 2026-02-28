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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.IoUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

class AwsRequestSigner
        implements HttpRequestInterceptor
{
    private static final String SERVICE_NAME = "es";
    private final AwsCredentialsProvider credentialsProvider;
    private final Aws4Signer signer;
    private final Region region;

    public AwsRequestSigner(String region, AwsCredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = credentialsProvider;
        this.region = Region.of(region);
        // Note: Aws4Signer is deprecated but we continue using it due to missing features
        // in AwsV4HttpSigner as documented in AWS SDK issue #5401
        // https://github.com/aws/aws-sdk-java-v2/issues/5401
        this.signer = Aws4Signer.create();
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

        Map<String, List<String>> headers = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        Arrays.stream(request.getAllHeaders()).forEach(header ->
                headers.put(header.getName(), List.of(header.getValue())));

        InputStream content = null;
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
            if (enclosingRequest.getEntity() != null) {
                content = enclosingRequest.getEntity().getContent();
            }
        }

        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host != null) {
            URI endpoint = URI.create(host.toURI());
            SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                    .method(SdkHttpMethod.fromValue(method))
                    .uri(endpoint.resolve(uri.getRawPath()))
                    .headers(headers);

            parameters.forEach((key, values) -> {
                for (String value : values) {
                    requestBuilder.putRawQueryParameter(key, value);
                }
            });

            // Using contentStreamProvider as per AWS SDK v2 API
            if (content != null) {
                try {
                    byte[] contentBytes = IoUtils.toByteArray(content);
                    ContentStreamProvider contentStreamProvider = () -> new ByteArrayInputStream(contentBytes);
                    requestBuilder.contentStreamProvider(contentStreamProvider);
                }
                catch (IOException e) {
                    throw new IOException("Failed to read request content", e);
                }
            }

            SdkHttpFullRequest sdkRequest = requestBuilder.build();

            Aws4SignerParams signerParams = Aws4SignerParams.builder()
                    .awsCredentials(credentialsProvider.resolveCredentials())
                    .signingName(SERVICE_NAME)
                    .signingRegion(region)
                    .build();
            SdkHttpFullRequest signedRequest = signer.sign(sdkRequest, signerParams);

            Header[] newHeaders = signedRequest.headers().entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream()
                            .map(value -> new BasicHeader(entry.getKey(), value)))
                    .toArray(Header[]::new);

            request.setHeaders(newHeaders);

            if (signedRequest.contentStreamProvider().isPresent() && request instanceof HttpEntityEnclosingRequest) {
                try {
                    InputStream signedContent = signedRequest.contentStreamProvider().get().newStream();
                    BasicHttpEntity entity = new BasicHttpEntity();
                    entity.setContent(signedContent);
                    ((HttpEntityEnclosingRequest) request).setEntity(entity);
                }
                catch (Exception e) {
                    throw new IOException("Failed to update request content after signing", e);
                }
            }
        }
    }
}
