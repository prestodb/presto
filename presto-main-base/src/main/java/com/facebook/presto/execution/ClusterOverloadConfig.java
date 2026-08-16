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
package com.facebook.presto.execution;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class ClusterOverloadConfig
{
    public static final String OVERLOAD_POLICY_CNT_BASED = "overload_worker_cnt_based_throttling";
    public static final String OVERLOAD_POLICY_PCT_BASED = "overload_worker_pct_based_throttling";
    private static final Splitter BYPASS_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final Splitter GROUP_ID_SEGMENT_SPLITTER = Splitter.on('.');

    private boolean clusterOverloadThrottlingEnabled;
    private double allowedOverloadWorkersPct = 0.01;
    private int allowedOverloadWorkersCnt;
    private String overloadPolicyType = OVERLOAD_POLICY_CNT_BASED;
    private int overloadCheckCacheTtlInSecs = 5;
    private Set<ResourceGroupId> throttlingBypassResourceGroups = ImmutableSet.of();

    /**
     * Gets the time-to-live for the cached cluster overload state.
     * This determines how frequently the system will re-evaluate whether the cluster is overloaded.
     *
     * @return the cache TTL duration
     */
    public int getOverloadCheckCacheTtlInSecs()
    {
        return overloadCheckCacheTtlInSecs;
    }

    /**
     * Gets the time-to-live for the cached cluster overload state.
     * This determines how frequently the system will re-evaluate whether the cluster is overloaded.
     *
     * @return the cache TTL duration
     */
    public int getOverloadCheckCacheTtlMillis()
    {
        return overloadCheckCacheTtlInSecs * 1000;
    }

    /**
     * Sets the time-to-live for the cached cluster overload state.
     *
     * @param overloadCheckCacheTtlInSecs the cache TTL duration
     * @return this for chaining
     */
    @Config("cluster.overload-check-cache-ttl-secs")
    public ClusterOverloadConfig setOverloadCheckCacheTtlInSecs(int overloadCheckCacheTtlInSecs)
    {
        this.overloadCheckCacheTtlInSecs = overloadCheckCacheTtlInSecs;
        return this;
    }

    @Config("cluster-overload.enable-throttling")
    public ClusterOverloadConfig setClusterOverloadThrottlingEnabled(boolean clusterOverloadThrottlingEnabled)
    {
        this.clusterOverloadThrottlingEnabled = clusterOverloadThrottlingEnabled;
        return this;
    }

    public boolean isClusterOverloadThrottlingEnabled()
    {
        return this.clusterOverloadThrottlingEnabled;
    }

    @Config("cluster-overload.allowed-overload-workers-pct")
    public ClusterOverloadConfig setAllowedOverloadWorkersPct(Double allowedOverloadWorkersPct)
    {
        this.allowedOverloadWorkersPct = allowedOverloadWorkersPct;
        return this;
    }

    public double getAllowedOverloadWorkersPct()
    {
        return this.allowedOverloadWorkersPct;
    }

    @Config("cluster-overload.allowed-overload-workers-cnt")
    public ClusterOverloadConfig setAllowedOverloadWorkersCnt(int allowedOverloadWorkersCnt)
    {
        this.allowedOverloadWorkersCnt = allowedOverloadWorkersCnt;
        return this;
    }

    public double getAllowedOverloadWorkersCnt()
    {
        return this.allowedOverloadWorkersCnt;
    }

    @Config("cluster-overload.overload-policy-type")
    public ClusterOverloadConfig setOverloadPolicyType(String overloadPolicyType)
    {
        // validate
        this.overloadPolicyType = overloadPolicyType;
        return this;
    }

    public String getOverloadPolicyType()
    {
        return this.overloadPolicyType;
    }

    public Set<ResourceGroupId> getThrottlingBypassResourceGroups()
    {
        return throttlingBypassResourceGroups;
    }

    @Config("cluster-overload.bypass-resource-groups")
    @ConfigDescription("Comma-separated list of fully-qualified resource group ids that bypass cluster-overload throttling")
    public ClusterOverloadConfig setThrottlingBypassResourceGroups(String throttlingBypassResourceGroups)
    {
        if (throttlingBypassResourceGroups == null) {
            this.throttlingBypassResourceGroups = ImmutableSet.of();
            return this;
        }
        // ResourceGroupId rejects empty segments, so a malformed entry like "global..admin" fails fast
        // at startup. Airlift Bootstrap surfaces the failure with the property name.
        ImmutableSet.Builder<ResourceGroupId> builder = ImmutableSet.builder();
        for (String entry : BYPASS_SPLITTER.split(throttlingBypassResourceGroups)) {
            builder.add(new ResourceGroupId(ImmutableList.copyOf(GROUP_ID_SEGMENT_SPLITTER.split(entry))));
        }
        this.throttlingBypassResourceGroups = builder.build();
        return this;
    }
}
