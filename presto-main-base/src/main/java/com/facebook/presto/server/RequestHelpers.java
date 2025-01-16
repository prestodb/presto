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
package com.facebook.presto.server;

import com.facebook.airlift.http.client.Request.Builder;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

public class RequestHelpers
{
    private RequestHelpers()
    {
    }

    /**
     * Sets the request Content-Type/Accept headers for JSON or SMILE encoding based on the
     * given isBinaryTransportEnabled argument.
     */
    public static Builder setContentTypeHeaders(boolean isBinaryTransportEnabled, Builder requestBuilder)
    {
        if (isBinaryTransportEnabled) {
            return getBinaryTransportBuilder(requestBuilder);
        }
        return getJsonTransportBuilder(requestBuilder);
    }

    public static Builder getBinaryTransportBuilder(Builder requestBuilder)
    {
        return requestBuilder
                .setHeader(CONTENT_TYPE, APPLICATION_JACKSON_SMILE)
                .setHeader(ACCEPT, APPLICATION_JACKSON_SMILE);
    }

    public static Builder getJsonTransportBuilder(Builder requestBuilder)
    {
        return requestBuilder
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString());
    }
}
