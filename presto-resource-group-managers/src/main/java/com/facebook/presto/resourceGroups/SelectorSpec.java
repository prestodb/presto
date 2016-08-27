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
package com.facebook.presto.resourceGroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class SelectorSpec
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> sourceRegex;
    private final ResourceGroupIdTemplate group;

    @JsonCreator
    public SelectorSpec(
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("group") ResourceGroupIdTemplate group)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        this.group = requireNonNull(group, "group is null");
    }

    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    public ResourceGroupIdTemplate getGroup()
    {
        return group;
    }
}
