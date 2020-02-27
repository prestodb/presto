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

import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.stream.Stream.concat;

public class FailureResolverManagerFactory
{
    private final Set<FailureResolver> failureResolvers;
    private final Set<FailureResolverFactory> failureResolverFactories;

    @Inject
    public FailureResolverManagerFactory(
            Set<FailureResolver> failureResolvers,
            Set<FailureResolverFactory> failureResolverFactories)
    {
        this.failureResolvers = ImmutableSet.copyOf(failureResolvers);
        this.failureResolverFactories = ImmutableSet.copyOf(failureResolverFactories);
    }

    public FailureResolverManager create(FailureResolverFactoryContext context)
    {
        Set<FailureResolver> allResolvers = concat(
                failureResolvers.stream(),
                failureResolverFactories.stream()
                        .map(factory -> factory.create(context)))
                .collect(toImmutableSet());
        return new FailureResolverManager(allResolvers);
    }
}
