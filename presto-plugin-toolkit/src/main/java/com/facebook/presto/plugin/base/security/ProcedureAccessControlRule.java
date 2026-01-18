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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class ProcedureAccessControlRule
{
    private final Set<ProcedurePrivilege> privileges;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> schemaRegex;
    private final Optional<Pattern> procedureRegex;

    @JsonCreator
    public ProcedureAccessControlRule(
            @JsonProperty("privileges") Set<ProcedurePrivilege> privileges,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("schema") Optional<Pattern> schemaRegex,
            @JsonProperty("procedure") Optional<Pattern> procedureRegex)
    {
        this.privileges = ImmutableSet.copyOf(requireNonNull(privileges, "privileges is null"));
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.schemaRegex = requireNonNull(schemaRegex, "schemaRegex is null");
        this.procedureRegex = requireNonNull(procedureRegex, "procedureRegex is null");
    }

    public Optional<Set<ProcedurePrivilege>> match(String user, SchemaTableName procedure)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                schemaRegex.map(regex -> regex.matcher(procedure.getSchemaName()).matches()).orElse(true) &&
                procedureRegex.map(regex -> regex.matcher(procedure.getTableName()).matches()).orElse(true)) {
            return Optional.of(privileges);
        }
        return Optional.empty();
    }

    public enum ProcedurePrivilege
    {
        EXECUTE
    }
}
