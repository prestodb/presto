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
package com.facebook.presto.ttl.clusterttlprovidermanagers;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

public class ClusterTtlProviderManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ClusterTtlProviderManagerConfig config = buildConfigObject(ClusterTtlProviderManagerConfig.class);
        switch (config.getClusterTtlProviderManagerType()) {
            case THROWING:
                binder.bind(ClusterTtlProviderManager.class).to(ThrowingClusterTtlProviderManager.class).in(Scopes.SINGLETON);
                break;
            case CONFIDENCE:
                binder.bind(ClusterTtlProviderManager.class).to(ConfidenceBasedClusterTtlProviderManager.class).in(Scopes.SINGLETON);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster Ttl provider manager " + config.getClusterTtlProviderManagerType());
        }
    }
}
