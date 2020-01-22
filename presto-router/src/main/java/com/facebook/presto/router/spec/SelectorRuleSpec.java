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
package com.facebook.presto.router.spec;

import com.facebook.presto.router.cluster.RequestInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class SelectorRuleSpec
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final Optional<List<String>> clientTags;
    private final String targetGroup;

    @JsonCreator
    public SelectorRuleSpec(
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("clientTags") Optional<List<String>> clientTags,
            @JsonProperty("targetGroup") String targetGroup)
    {
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.targetGroup = requireNonNull(targetGroup, "targetGroup is null");
    }

    @JsonProperty
    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    @JsonProperty
    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    @JsonProperty
    public Optional<List<String>> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    public String getTargetGroup()
    {
        return targetGroup;
    }

    public Optional<String> match(RequestInfo requestInfo)
    {
        if (userRegex.isPresent()) {
            Matcher userMatcher = userRegex.get().matcher(requestInfo.getUser());
            if (!userMatcher.matches()) {
                return Optional.empty();
            }
        }

        if (sourceRegex.isPresent()) {
            String source = requestInfo.getSource().orElse("");
            if (!sourceRegex.get().matcher(source).matches()) {
                return Optional.empty();
            }
        }

        if (clientTags.isPresent() && requestInfo.getClientTags().containsAll(clientTags.get())) {
            return Optional.empty();
        }

        // all selector criteria matches, return the target group
        return Optional.of(targetGroup);
    }
}
