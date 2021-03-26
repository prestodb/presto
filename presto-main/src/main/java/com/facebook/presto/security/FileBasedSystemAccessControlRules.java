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

import com.facebook.presto.plugin.base.security.SchemaAccessControlRule;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class FileBasedSystemAccessControlRules
{
    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;
    private final Optional<List<SchemaAccessControlRule>> schemaRules;

    @JsonCreator
    public FileBasedSystemAccessControlRules(
            @JsonProperty("catalogs") Optional<List<CatalogAccessControlRule>> catalogRules,
            @JsonProperty("principals") Optional<List<PrincipalUserMatchRule>> principalUserMatchRules,
            @JsonProperty("schemas") Optional<List<SchemaAccessControlRule>> schemaRules)
    {
        this.catalogRules = catalogRules.map(ImmutableList::copyOf).orElse(ImmutableList.of());
        this.principalUserMatchRules = principalUserMatchRules.map(ImmutableList::copyOf);
        this.schemaRules = schemaRules.map(ImmutableList::copyOf);
    }

    public List<CatalogAccessControlRule> getCatalogRules()
    {
        return catalogRules;
    }

    public Optional<List<PrincipalUserMatchRule>> getPrincipalUserMatchRules()
    {
        return principalUserMatchRules;
    }

    public Optional<List<SchemaAccessControlRule>> getSchemaRules()
    {
        return schemaRules;
    }
}
