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
package io.prestosql.plugin.base.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class SessionPropertyAccessControlRule
{
    private final boolean allow;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> propertyRegex;

    @JsonCreator
    public SessionPropertyAccessControlRule(
            @JsonProperty("allow") boolean allow,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("property") Optional<Pattern> propertyRegex)
    {
        this.allow = allow;
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.propertyRegex = requireNonNull(propertyRegex, "propertyRegex is null");
    }

    public Optional<Boolean> match(String user, String property)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                propertyRegex.map(regex -> regex.matcher(property).matches()).orElse(true)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
