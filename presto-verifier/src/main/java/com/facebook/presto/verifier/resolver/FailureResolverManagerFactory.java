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

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FailureResolverManagerFactory
{
    private final List<FailureResolverFactory> failureResolverFactories;
    private final boolean enabled;

    @Inject
    public FailureResolverManagerFactory(List<FailureResolverFactory> failureResolverFactories, FailureResolverConfig failureResolverConfig)
    {
        this.failureResolverFactories = requireNonNull(failureResolverFactories, "failureResolverFactories is null");
        this.enabled = failureResolverConfig.isEnabled();
    }

    public FailureResolverManager create()
    {
        List<FailureResolver> failureResolvers = ImmutableList.of();
        if (enabled) {
            failureResolvers = failureResolverFactories.stream()
                    .map(FailureResolverFactory::create)
                    .collect(toImmutableList());
        }
        return new FailureResolverManager(failureResolvers);
    }
}
