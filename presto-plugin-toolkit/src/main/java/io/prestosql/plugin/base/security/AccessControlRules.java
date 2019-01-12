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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class AccessControlRules
{
    private final List<SchemaAccessControlRule> schemaRules;
    private final List<TableAccessControlRule> tableRules;
    private final List<SessionPropertyAccessControlRule> sessionPropertyRules;

    @JsonCreator
    public AccessControlRules(
            @JsonProperty("schemas") Optional<List<SchemaAccessControlRule>> schemaRules,
            @JsonProperty("tables") Optional<List<TableAccessControlRule>> tableRules,
            @JsonProperty("sessionProperties") Optional<List<SessionPropertyAccessControlRule>> sessionPropertyRules)
    {
        this.schemaRules = schemaRules.orElse(ImmutableList.of());
        this.tableRules = tableRules.orElse(ImmutableList.of());
        this.sessionPropertyRules = sessionPropertyRules.orElse(ImmutableList.of());
    }

    public List<SchemaAccessControlRule> getSchemaRules()
    {
        return schemaRules;
    }

    public List<TableAccessControlRule> getTableRules()
    {
        return tableRules;
    }

    public List<SessionPropertyAccessControlRule> getSessionPropertyRules()
    {
        return sessionPropertyRules;
    }
}
