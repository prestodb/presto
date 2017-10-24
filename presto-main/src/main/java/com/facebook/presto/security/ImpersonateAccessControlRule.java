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
package com.facebook.presto.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class ImpersonateAccessControlRule
{
    private final boolean allow;
    private final Optional<Pattern> principleRegex;
    private final Optional<Pattern> userRegex;

    @JsonCreator
    public ImpersonateAccessControlRule(
            @JsonProperty("allow") boolean allow,
            @JsonProperty("principle") Optional<Pattern> principleRegex,
            @JsonProperty("user") Optional<Pattern> userRegex)
    {
        this.allow = allow;
        this.principleRegex = requireNonNull(principleRegex, "principleRegex is null");
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
    }

    public Optional<Boolean> match(String principle, String user)
    {
        if (principleRegex.map(regex -> regex.matcher(principle).matches()).orElse(true) &&
                userRegex.map(regex -> regex.matcher(user).matches()).orElse(true)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
