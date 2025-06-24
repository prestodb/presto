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

import com.facebook.presto.spi.router.RouterRequestInfo;
import com.facebook.presto.spi.router.Scheduler;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class PlanCheckerRouterPluginScheduler
        implements Scheduler
{
    private final AtomicInteger planCheckerClusterCandidateIndex = new AtomicInteger(0);

    private List<URI> candidates;
    private final List<URI> planCheckerClusterCandidates;
    private final URI javaRouterURI;
    private final URI nativeRouterURI;
    private final Duration clientRequestTimeout;
    private final boolean javaClusterFallbackEnabled;

    @Inject
    public PlanCheckerRouterPluginScheduler(PlanCheckerRouterPluginConfig planCheckerRouterConfig)
    {
        requireNonNull(planCheckerRouterConfig, "PlanCheckerRouterPluginConfig is null");
        this.planCheckerClusterCandidates =
                requireNonNull(planCheckerRouterConfig.getPlanCheckClustersURIs(), "validatorCandidates is null");
        this.javaRouterURI =
                requireNonNull(planCheckerRouterConfig.getJavaRouterURI(), "javaRouterURI is null");
        this.nativeRouterURI =
                requireNonNull(planCheckerRouterConfig.getNativeRouterURI(), "nativeRouterURI is null");
        this.clientRequestTimeout = planCheckerRouterConfig.getClientRequestTimeout();
        this.javaClusterFallbackEnabled = planCheckerRouterConfig.isJavaClusterFallbackEnabled();
    }

    @Override
    public Optional<URI> getDestination(RouterRequestInfo routerRequestInfo)
    {
        PlanCheckerRouterPluginPrestoClient planCheckerPrestoClient = new PlanCheckerRouterPluginPrestoClient(getValidatorDestination(), javaRouterURI, nativeRouterURI, clientRequestTimeout, javaClusterFallbackEnabled);
        return planCheckerPrestoClient.getCompatibleClusterURI(routerRequestInfo.getHeaders(), routerRequestInfo.getQuery(), routerRequestInfo.getPrincipal());
    }

    @Override
    public void setCandidates(List<URI> candidates)
    {
        this.candidates = candidates;
    }

    private URI getValidatorDestination()
    {
        int currentIndex = planCheckerClusterCandidateIndex.getAndUpdate(i -> (i + 1) % planCheckerClusterCandidates.size());
        return planCheckerClusterCandidates.get(currentIndex);
    }
}
