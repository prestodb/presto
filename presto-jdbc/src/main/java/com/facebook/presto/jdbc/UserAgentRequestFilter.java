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
package com.facebook.presto.jdbc;

import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.Request.Builder.fromRequest;

class UserAgentRequestFilter
        implements HttpRequestFilter
{
    private final String userAgent;

    public UserAgentRequestFilter(String userAgent)
    {
        this.userAgent = checkNotNull(userAgent, "userAgent is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader(HttpHeaders.USER_AGENT, userAgent)
                .build();
    }
}
