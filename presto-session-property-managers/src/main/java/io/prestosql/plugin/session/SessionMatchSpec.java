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
package io.prestosql.plugin.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.session.SessionConfigurationContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class SessionMatchSpec
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final Set<String> clientTags;
    private final Optional<String> queryType;
    private final Optional<Pattern> resourceGroupRegex;
    private final Map<String, String> sessionProperties;

    @JsonCreator
    public SessionMatchSpec(
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("clientTags") Optional<List<String>> clientTags,
            @JsonProperty("queryType") Optional<String> queryType,
            @JsonProperty("group") Optional<Pattern> resourceGroupRegex,
            @JsonProperty("sessionProperties") Map<String, String> sessionProperties)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        requireNonNull(clientTags, "clientTags is null");
        this.clientTags = ImmutableSet.copyOf(clientTags.orElse(ImmutableList.of()));
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.resourceGroupRegex = requireNonNull(resourceGroupRegex, "resourceGroupRegex is null");
        requireNonNull(sessionProperties, "sessionProperties is null");
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
    }

    public Map<String, String> match(SessionConfigurationContext context)
    {
        if (userRegex.isPresent() && !userRegex.get().matcher(context.getUser()).matches()) {
            return ImmutableMap.of();
        }
        if (sourceRegex.isPresent()) {
            String source = context.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return ImmutableMap.of();
            }
        }
        if (!clientTags.isEmpty() && !context.getClientTags().containsAll(clientTags)) {
            return ImmutableMap.of();
        }

        if (queryType.isPresent()) {
            String contextQueryType = context.getQueryType().orElse("");
            if (!queryType.get().equalsIgnoreCase(contextQueryType)) {
                return ImmutableMap.of();
            }
        }

        if (resourceGroupRegex.isPresent() && !resourceGroupRegex.get().matcher(context.getResourceGroupId().toString()).matches()) {
            return ImmutableMap.of();
        }

        return sessionProperties;
    }

    @JsonProperty("user")
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    @JsonProperty("source")
    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    @JsonProperty
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    public Optional<String> getQueryType()
    {
        return queryType;
    }

    @JsonProperty("group")
    public Optional<Pattern> getResourceGroupRegex()
    {
        return resourceGroupRegex;
    }

    @JsonProperty
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }
}
