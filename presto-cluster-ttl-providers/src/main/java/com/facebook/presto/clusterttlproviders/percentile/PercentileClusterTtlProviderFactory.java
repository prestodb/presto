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
package com.facebook.presto.clusterttlproviders.percentile;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.spi.ttl.ClusterTtlProvider;
import com.facebook.presto.spi.ttl.ClusterTtlProviderFactory;
import com.google.inject.Injector;

import java.util.Map;

public class PercentileClusterTtlProviderFactory
        implements ClusterTtlProviderFactory
{
    @Override
    public String getName()
    {
        return "percentile";
    }

    @Override
    public ClusterTtlProvider create(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(new PercentileBasedClusterTtlProviderModule());
            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(ClusterTtlProvider.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
