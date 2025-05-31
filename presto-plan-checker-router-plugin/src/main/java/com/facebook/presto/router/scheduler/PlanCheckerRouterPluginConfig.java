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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.Iterables.transform;

public class PlanCheckerRouterPluginConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<URI> planCheckClustersURIs;
    private URI javaRouterURI;
    private URI nativeRouterURI;

    @Config("plan-check-clusters-uris")
    public PlanCheckerRouterPluginConfig setPlanCheckClustersURIs(String uris)
    {
        if (uris == null) {
            this.planCheckClustersURIs = null;
            return this;
        }

        this.planCheckClustersURIs = ImmutableList.copyOf(transform(SPLITTER.split(uris), URI::create));
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
}
