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
package com.facebook.presto.hive.security.ranger;

import com.facebook.airlift.http.client.BasicAuthRequestFilter;
import com.facebook.airlift.http.client.HttpRequestFilter;
import com.facebook.airlift.http.client.Request;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class RangerBasicAuthHttpRequestFilter
        implements HttpRequestFilter
{
    private final BasicAuthRequestFilter filter;

    @Inject
    public RangerBasicAuthHttpRequestFilter(RangerBasedAccessControlConfig config)
    {
        String username = requireNonNull(config.getBasicAuthUser(), "username cannot be null when using basic authentication");
        String password = requireNonNull(config.getBasicAuthPassword(), "password cannot be null when using basic authentication");
        this.filter = new BasicAuthRequestFilter(username, password);
    }

    @Override
    public Request filterRequest(Request request)
    {
        return filter.filterRequest(request);
    }
}
