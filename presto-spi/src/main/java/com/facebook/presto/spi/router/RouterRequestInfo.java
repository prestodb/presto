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
package com.facebook.presto.spi.router;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class RouterRequestInfo
{
    private final String user;
    private final Optional<String> source;
    private final List<String> clientTags;
    private final String query;
    private final Object servletRequest;

    public RouterRequestInfo(String user)
    {
        this(user, Optional.empty(), Collections.emptyList(), "", null);
    }

    public RouterRequestInfo(String user, Optional<String> source, List<String> clientTags, String query, Object servletRequest)
    {
        this.user = user;
        this.source = source;
        this.clientTags = clientTags;
        this.query = query;
        this.servletRequest = servletRequest;
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public String getQuery()
    {
        return query;
    }

    public List<String> getClientTags()
    {
        return clientTags;
    }

    public Object getServletRequest()
    {
        return servletRequest;
    }
}
