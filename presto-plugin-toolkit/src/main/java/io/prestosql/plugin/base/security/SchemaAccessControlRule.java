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
package com.facebook.presto.plugin.base.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class SchemaAccessControlRule
{
    private final boolean owner;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> schemaRegex;

    @JsonCreator
    public SchemaAccessControlRule(
            @JsonProperty("owner") boolean owner,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex)
    {
        this.owner = owner;
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.schemaRegex = requireNonNull(schemaRegex, "sourceRegex is null");
    }

    public Optional<Boolean> match(String user, String schema)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                schemaRegex.map(regex -> regex.matcher(schema).matches()).orElse(true)) {
            return Optional.of(owner);
        }
        return Optional.empty();
    }
}
