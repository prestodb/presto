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

import org.apache.hc.client5.http.async.AsyncExecCallback;
import org.apache.hc.client5.http.async.AsyncExecChain;
import org.apache.hc.client5.http.async.AsyncExecChainHandler;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.nio.AsyncDataConsumer;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;

import java.io.IOException;

/**
 * Removes {@code Content-Encoding} and {@code Content-Length} from responses that httpclient5 has
 * already decoded.
 *
 * <p>httpclient5's {@code ContentCompressionAsyncExec} inflates the response body but only clears
 * {@code Content-Encoding} on the {@link EntityDetails} it hands downstream; the header stays on the
 * message itself (see {@code ContentCompressionAsyncExec.wrapEntityDetails}, httpclient5 5.6.x).
 * elasticsearch-java's {@code Rest5Client.convertResponse} then reads that stale header and wraps the
 * already-inflated body in a {@code GzipDecompressingEntity}, so reading it fails with
 * "Not in GZIP format".
 *
 * <p>Registered as the outermost exec interceptor so it runs after the compression exec has decoded
 * the body. Only needed when content compression is enabled; when it is disabled no response carries
 * a {@code Content-Encoding} we are responsible for.
 */
class StripStaleContentEncodingExec
        implements AsyncExecChainHandler
{
    @Override
    public void execute(
            HttpRequest request,
            AsyncEntityProducer entityProducer,
            AsyncExecChain.Scope scope,
            AsyncExecChain chain,
            AsyncExecCallback asyncExecCallback)
            throws HttpException, IOException
    {
        chain.proceed(request, entityProducer, scope, new AsyncExecCallback()
        {
            @Override
            public AsyncDataConsumer handleResponse(HttpResponse response, EntityDetails entityDetails)
                    throws HttpException, IOException
            {
                // entityDetails reports no content encoding once the compression exec has decoded the
                // body; that is the signal the message headers are now stale.
                if (entityDetails != null && entityDetails.getContentEncoding() == null) {
                    response.removeHeaders(HttpHeaders.CONTENT_ENCODING);
                    response.removeHeaders(HttpHeaders.CONTENT_LENGTH);
                }
                return asyncExecCallback.handleResponse(response, entityDetails);
            }

            @Override
            public void handleInformationResponse(HttpResponse response)
                    throws HttpException, IOException
            {
                asyncExecCallback.handleInformationResponse(response);
            }

            @Override
            public void completed()
            {
                asyncExecCallback.completed();
            }

            @Override
            public void failed(Exception cause)
            {
                asyncExecCallback.failed(cause);
            }
        });
    }
}
