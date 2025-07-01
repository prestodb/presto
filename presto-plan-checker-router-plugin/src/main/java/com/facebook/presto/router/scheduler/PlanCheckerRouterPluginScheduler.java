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

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PlanCheckerRouterPluginScheduler
        implements Scheduler
{
    private List<URI> candidates;
    private final PlanCheckerRouterPluginPrestoClient planCheckerRouterPluginPrestoClient;

    @Inject
    public PlanCheckerRouterPluginScheduler(PlanCheckerRouterPluginPrestoClient planCheckerRouterPluginPrestoClient)
    {
        this.planCheckerRouterPluginPrestoClient =
                requireNonNull(planCheckerRouterPluginPrestoClient, "planCheckerRouterPluginPrestoClient is null");
    }

    @Override
    public Optional<URI> getDestination(RouterRequestInfo routerRequestInfo)
    {
        return planCheckerRouterPluginPrestoClient.getCompatibleClusterURI(routerRequestInfo.getHeaders(), routerRequestInfo.getQuery(), routerRequestInfo.getPrincipal());
    }

    @Override
    public void setCandidates(List<URI> candidates)
    {
        this.candidates = candidates;
    }
}
