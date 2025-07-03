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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import com.google.common.base.Splitter;
import com.google.common.collect.Streams;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.MINUTES;

public class PlanCheckerRouterPluginConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<URI> planCheckClustersURIs;
    private URI javaRouterURI;
    private URI nativeRouterURI;
    private Duration clientRequestTimeout = new Duration(2, MINUTES);
    private boolean javaClusterFallbackEnabled;

    @Config("plan-check-clusters-uris")
    public PlanCheckerRouterPluginConfig setPlanCheckClustersURIs(String uris)
    {
        if (uris == null) {
            this.planCheckClustersURIs = null;
            return this;
        }

        this.planCheckClustersURIs = Streams.stream(SPLITTER.split(uris))
                .map(URI::create)
                .collect(toImmutableList());
        return this;
    }

    public List<URI> getPlanCheckClustersURIs()
    {
        return planCheckClustersURIs;
    }

    @Config("router-java-url")
    public PlanCheckerRouterPluginConfig setJavaRouterURI(URI javaRouterURI)
    {
        this.javaRouterURI = javaRouterURI;
        return this;
    }

    public URI getJavaRouterURI()
    {
        return javaRouterURI;
    }

    @Config("router-native-url")
    public PlanCheckerRouterPluginConfig setNativeRouterURI(URI nativeRouterURI)
    {
        this.nativeRouterURI = nativeRouterURI;
        return this;
    }

    public URI getNativeRouterURI()
    {
        return nativeRouterURI;
    }

    @MinDuration("0ns")
    public Duration getClientRequestTimeout()
    {
        return clientRequestTimeout;
    }

    @Config("client-request-timeout")
    public PlanCheckerRouterPluginConfig setClientRequestTimeout(Duration clientRequestTimeout)
    {
        this.clientRequestTimeout = clientRequestTimeout;
        return this;
    }

    public boolean isJavaClusterFallbackEnabled()
    {
        return javaClusterFallbackEnabled;
    }

    @Config("enable-java-cluster-fallback")
    public PlanCheckerRouterPluginConfig setJavaClusterFallbackEnabled(boolean javaClusterFallbackEnabled)
    {
        this.javaClusterFallbackEnabled = javaClusterFallbackEnabled;
        return this;
    }
}
