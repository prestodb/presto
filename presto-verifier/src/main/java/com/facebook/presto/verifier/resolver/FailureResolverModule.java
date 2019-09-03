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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;

import java.util.List;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class FailureResolverModule
        implements Module
{
    private final List<FailureResolverFactory> failureResolverFactories;

    public FailureResolverModule(List<FailureResolverFactory> failureResolverFactories)
    {
        this.failureResolverFactories = requireNonNull(failureResolverFactories, "failureResolverFactories is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(FailureResolverConfig.class);

        binder.bind(new TypeLiteral<List<FailureResolverFactory>>() {}).toInstance(failureResolverFactories);
        binder.bind(FailureResolverManagerFactory.class).in(SINGLETON);
    }
}
