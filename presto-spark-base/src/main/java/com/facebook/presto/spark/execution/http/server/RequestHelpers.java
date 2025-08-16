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
package com.facebook.presto.spark.execution.http.server;

import okhttp3.Request;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;

public class RequestHelpers
{
    private RequestHelpers()
    {
    }

    /**
     * Sets the request Content-Type/Accept headers for JSON or SMILE encoding based on the
     * given isBinaryTransportEnabled argument.
     */
    public static Request.Builder setContentTypeHeaders(boolean isBinaryTransportEnabled, Request.Builder requestBuilder)
    {
        if (isBinaryTransportEnabled) {
            return getBinaryTransportBuilder(requestBuilder);
        }
        return getJsonTransportBuilder(requestBuilder);
    }

    public static Request.Builder setTaskUpdateRequestContentTypeHeaders(boolean isTaskUpdateRequestThriftTransportEnabled, boolean isBinaryTransportEnabled, Request.Builder requestBuilder)
    {
        if (isTaskUpdateRequestThriftTransportEnabled) {
            requestBuilder.addHeader(CONTENT_TYPE, "application/x-thrift+binary");
        }
        else if (isBinaryTransportEnabled) {
            requestBuilder.addHeader(CONTENT_TYPE, APPLICATION_JACKSON_SMILE);
        }
        else {
            requestBuilder.addHeader(CONTENT_TYPE, JSON_UTF_8.toString());
        }

        return requestBuilder;
    }

    public static Request.Builder setTaskInfoAcceptTypeHeaders(boolean isTaskInfoThriftTransportEnabled, boolean isBinaryTransportEnabled, Request.Builder requestBuilder)
    {
        if (isTaskInfoThriftTransportEnabled) {
            requestBuilder.addHeader(ACCEPT, "application/x-thrift+binary");
        }
        else if (isBinaryTransportEnabled) {
            requestBuilder.addHeader(ACCEPT, APPLICATION_JACKSON_SMILE);
        }
        else {
            requestBuilder.addHeader(ACCEPT, JSON_UTF_8.toString());
        }
        return requestBuilder;
    }

    public static Request.Builder getBinaryTransportBuilder(Request.Builder requestBuilder)
    {
        return requestBuilder
                .addHeader(CONTENT_TYPE, APPLICATION_JACKSON_SMILE)
                .addHeader(ACCEPT, APPLICATION_JACKSON_SMILE);
    }

    public static Request.Builder getJsonTransportBuilder(Request.Builder requestBuilder)
    {
        return requestBuilder
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(ACCEPT, JSON_UTF_8.toString());
    }
}
