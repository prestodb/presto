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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class SimpleTtlNodeSelectorConfig
{
    private boolean useDefaultExecutionTimeEstimateAsFallback;
    private Duration defaultExecutionTimeEstimate = new Duration(30, TimeUnit.MINUTES);

    public boolean getUseDefaultExecutionTimeEstimateAsFallback()
    {
        return useDefaultExecutionTimeEstimateAsFallback;
    }

    @Config("simple-ttl-node-selector.use-default-execution-time-estimate-as-fallback")
    public SimpleTtlNodeSelectorConfig setUseDefaultExecutionTimeEstimateAsFallback(boolean useDefaultExecutionTimeEstimateAsFallback)
    {
        this.useDefaultExecutionTimeEstimateAsFallback = useDefaultExecutionTimeEstimateAsFallback;
        return this;
    }

    public Duration getDefaultExecutionTimeEstimate()
    {
        return defaultExecutionTimeEstimate;
    }

    @Config("simple-ttl-node-selector.default-execution-time-estimate")
    public SimpleTtlNodeSelectorConfig setDefaultExecutionTimeEstimate(Duration defaultExecutionTimeEstimate)
    {
        this.defaultExecutionTimeEstimate = defaultExecutionTimeEstimate;
        return this;
    }
}
