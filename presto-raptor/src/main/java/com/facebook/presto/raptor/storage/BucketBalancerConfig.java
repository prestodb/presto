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

package com.facebook.presto.raptor.storage;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class BucketBalancerConfig
{
    private boolean balancerEnabled = true;
    private Duration balancerInterval = new Duration(6, TimeUnit.HOURS);

    public boolean isBalancerEnabled()
    {
        return balancerEnabled;
    }

    @Config("storage.balancer-enabled")
    public BucketBalancerConfig setBalancerEnabled(boolean balancerEnabled)
    {
        this.balancerEnabled = balancerEnabled;
        return this;
    }

    public Duration getBalancerInterval()
    {
        return balancerInterval;
    }

    @Config("storage.balancer-interval")
    @ConfigDescription("How often to run the global bucket balancer")
    public BucketBalancerConfig setBalancerInterval(Duration balancerInterval)
    {
        this.balancerInterval = balancerInterval;
        return this;
    }
}
