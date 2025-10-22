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
package com.facebook.presto.execution.scheduler.clusterOverload;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.google.inject.Binder;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;

/**
 * Provides bindings for the node overload policy and cluster resource checker.
 */
public class ClusterOverloadPolicyModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Bind the default node overload policy
        binder.bind(ClusterOverloadPolicy.class).to(CpuMemoryOverloadPolicy.class).in(SINGLETON);

        // Bind the node overload policy factory
        binder.bind(ClusterOverloadPolicyFactory.class).in(SINGLETON);

        // Bind the cluster resource checker
        binder.bind(ClusterResourceChecker.class).in(SINGLETON);

        // Bind the cluster overload config
        configBinder(binder).bindConfig(ClusterOverloadConfig.class);
    }
}
