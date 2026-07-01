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
package com.facebook.presto.tvf;

import com.facebook.airlift.http.client.HttpClient;

/**
 * Holder for HTTP client instance.
 */
public final class HttpClientHolder
{
    private static HttpClient httpClient;

    /**
     * Private constructor to prevent instantiation.
     */
    private HttpClientHolder()
    {
    }

    /**
     * Sets the HTTP client.
     *
     * @param httpClient the HTTP client
     */
    public static void setHttpClient(
            @ForWorkerInfo final HttpClient httpClient)
    {
        HttpClientHolder.httpClient = httpClient;
    }

    /**
     * Gets the HTTP client.
     *
     * @return the HTTP client
     */
    public static HttpClient getHttpClient()
    {
        return httpClient;
    }
}
