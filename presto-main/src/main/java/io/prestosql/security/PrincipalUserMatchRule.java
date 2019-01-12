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
package io.prestosql.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PrincipalUserMatchRule
{
    private final Pattern principalRegex;
    private final Optional<Pattern> userRegex;
    private final Optional<String> principalToUserSubstitution;
    private final boolean allow;

    @JsonCreator
    public PrincipalUserMatchRule(
            @JsonProperty("principal") Pattern principalRegex,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("principal_to_user") Optional<String> principalToUserSubstitution,
            @JsonProperty("allow") boolean allow)
    {
        this.principalRegex = requireNonNull(principalRegex, "principalRegex is null");
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.principalToUserSubstitution = requireNonNull(principalToUserSubstitution, "principalToUserSubstitution is null");
        checkState(
                userRegex.isPresent() || principalToUserSubstitution.isPresent(),
                "A valid principal rule must provide at least one criterion of user and user_extraction");
        this.allow = allow;
    }

    public Optional<Boolean> match(String principal, String user)
    {
        Matcher matcher = principalRegex.matcher(principal);

        if (!matcher.matches()) {
            return Optional.empty();
        }

        if (userRegex.isPresent()) {
            if (userRegex.get().matcher(user).matches()) {
                return Optional.of(allow);
            }
        }

        if (principalToUserSubstitution.isPresent()) {
            String userExtraction = matcher.replaceAll(principalToUserSubstitution.get());
            if (user.equals(userExtraction)) {
                return Optional.of(allow);
            }
        }

        return Optional.empty();
    }
}
