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
package com.facebook.presto.verifier.resolver;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;

public class IgnoredFunctionsMismatchResolverConfig
{
    private Set<String> functions = ImmutableSet.of();

    public Set<String> getFunctions()
    {
        return functions;
    }

    @Config("functions")
    @ConfigDescription("A comma-separated list of ignored functions")
    public IgnoredFunctionsMismatchResolverConfig setFunctions(String functions)
    {
        if (functions != null) {
            this.functions = Splitter.on(",").trimResults().splitToList(functions).stream()
                    .map(catalog -> catalog.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }
        return this;
    }
}
