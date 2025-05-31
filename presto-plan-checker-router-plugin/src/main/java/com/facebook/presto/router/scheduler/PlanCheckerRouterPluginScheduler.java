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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.router.RequestInfo;
import com.facebook.presto.spi.router.Scheduler;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PlanCheckerRouterPluginScheduler
        implements Scheduler
{
    private static Integer planCheckerClusterCandidateIndex = 0;
    private static final Logger log = Logger.get(PlanCheckerRouterPluginScheduler.class);

    private List<URI> candidates;
    private final List<URI> planCheckerClusterCandidates;
    private final URI javaRouterURI;
    private final URI nativeRouterURI;

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
    }

    @Override
    public Optional<URI> getDestination(RequestInfo requestInfo)
    {
        PlanCheckerRouterPluginPrestoClient planCheckerPrestoClient = new PlanCheckerRouterPluginPrestoClient(getValidatorDestination(requestInfo.getUser()), javaRouterURI, nativeRouterURI);
        return planCheckerPrestoClient.getCompatibleClusterURI(requestInfo.getServletRequest(), requestInfo.getQuery());
    }

    @Override
    public void setCandidates(List<URI> candidates)
    {
        this.candidates = candidates;
    }

    private URI getValidatorDestination(String user)
    {
        try {
            synchronized (planCheckerClusterCandidateIndex) {
                if (planCheckerClusterCandidateIndex >= candidates.size()) {
                    planCheckerClusterCandidateIndex = 0;
                }
                return planCheckerClusterCandidates.get(planCheckerClusterCandidateIndex++);
            }
        }
        catch (IllegalArgumentException e) {
            log.warn(e, "Error getting validator destination for user " + user);
            return null;
        }
    }
}
