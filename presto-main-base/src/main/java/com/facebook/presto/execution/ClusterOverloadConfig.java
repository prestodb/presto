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

public class ClusterOverloadConfig
{
    public static final String OVERLOAD_POLICY_CNT_BASED = "overload_worker_cnt_based_throttling";
    public static final String OVERLOAD_POLICY_PCT_BASED = "overload_worker_pct_based_throttling";
    private boolean clusterOverloadThrottlingEnabled;
    private double allowedOverloadWorkersPct = 0.01;
    private double allowedOverloadWorkersCnt = 1;
    private String overloadPolicyType = OVERLOAD_POLICY_CNT_BASED;

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
    public ClusterOverloadConfig setAllowedOverloadWorkersCnt(Double allowedOverloadWorkersCnt)
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
}
