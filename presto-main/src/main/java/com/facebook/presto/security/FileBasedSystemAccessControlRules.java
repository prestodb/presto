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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileBasedSystemAccessControlRules
{
    private final List<CatalogAccessControlRule> catalogRules;
    private final KerberosPrincipalAccessControlRule kerberosPrincipalRule;

    @JsonCreator
    public FileBasedSystemAccessControlRules(
            @JsonProperty("catalogs") Optional<List<CatalogAccessControlRule>> catalogRules,
            @JsonProperty("kerberosPrincipals") Optional<KerberosPrincipalAccessControlRule> kerberosPrincipalRule)
    {
        requireNonNull(catalogRules, "catalogRules is null");
        requireNonNull(kerberosPrincipalRule, "kerberosPrincipalRule is null");
        this.catalogRules = catalogRules.orElse(ImmutableList.of());
        this.kerberosPrincipalRule = kerberosPrincipalRule.orElse(new KerberosPrincipalAccessControlRule(false));
    }

    public List<CatalogAccessControlRule> getCatalogRules()
    {
        return catalogRules;
    }

    public KerberosPrincipalAccessControlRule getKerberosPrincipalRule()
    {
        return kerberosPrincipalRule;
    }
}
