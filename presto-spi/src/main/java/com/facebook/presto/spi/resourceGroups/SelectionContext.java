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
package com.facebook.presto.spi.resourceGroups;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class SelectionContext
{
    private final boolean authenticated;
    private final String user;
    private final Optional<String> source;
    private final int queryPriority;

    public SelectionContext(boolean authenticated, String user, Optional<String> source, int queryPriority)
    {
        this.authenticated = authenticated;
        this.user = requireNonNull(user, "user is null");
        this.source = requireNonNull(source, "source is null");
        this.queryPriority = queryPriority;
    }

    public boolean isAuthenticated()
    {
        return authenticated;
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public int getQueryPriority()
    {
        return queryPriority;
    }
}
