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

import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating ClusterOverloadPolicy instances.
 * This allows for extensible policy creation based on configuration.
 */
public class ClusterOverloadPolicyFactory
{
    private final Map<String, ClusterOverloadPolicy> policies;

    @Inject
    public ClusterOverloadPolicyFactory(ClusterOverloadPolicy clusterOverloadPolicy)
    {
        requireNonNull(clusterOverloadPolicy, "clusterOverloadPolicy is null");

        // Register available policies
        ImmutableMap.Builder<String, ClusterOverloadPolicy> policiesBuilder = ImmutableMap.builder();

        // Add the default overload policy - use the injected instance
        policiesBuilder.put(clusterOverloadPolicy.getName(), clusterOverloadPolicy);

        // Add more policies here as they are implemented
        this.policies = policiesBuilder.build();
    }

    /**
     * Get a policy by name.
     *
     * @param name The name of the policy to get
     * @return The policy, or empty if no policy with that name exists
     */
    public Optional<ClusterOverloadPolicy> getPolicy(String name)
    {
        return Optional.ofNullable(policies.get(name));
    }

    /**
     * Get the default policy.
     *
     * @return The default policy
     */
    public ClusterOverloadPolicy getDefaultPolicy()
    {
        // Default to CPU/Memory policy
        return policies.get("cpu-memory-overload");
    }

    /**
     * Get all available policies.
     *
     * @return Map of policy name to policy
     */
    public Map<String, ClusterOverloadPolicy> getPolicies()
    {
        return policies;
    }
}
