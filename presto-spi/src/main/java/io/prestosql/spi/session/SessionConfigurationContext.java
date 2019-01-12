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
package io.prestosql.spi.session;

import io.prestosql.spi.resourcegroups.ResourceGroupId;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public final class SessionConfigurationContext
{
    private final String user;
    private final Optional<String> source;
    private final Set<String> clientTags;
    private final Optional<String> queryType;
    private final ResourceGroupId resourceGroupId;

    public SessionConfigurationContext(String user, Optional<String> source, Set<String> clientTags, Optional<String> queryType, ResourceGroupId resourceGroupId)
    {
        this.user = requireNonNull(user, "user is null");
        this.source = requireNonNull(source, "source is null");
        this.clientTags = unmodifiableSet(new HashSet<>(requireNonNull(clientTags, "clientTags is null")));
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId");
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    public Optional<String> getQueryType()
    {
        return queryType;
    }

    public ResourceGroupId getResourceGroupId()
    {
        return resourceGroupId;
    }
}
