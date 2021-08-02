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

import javax.validation.constraints.Min;

public class NodeResourceStatusConfig
{
    private int requiredWorkersActive;

    @Min(0)
    public int getRequiredWorkersActive()
    {
        return requiredWorkersActive;
    }

    @Config("cluster.required-workers-active")
    @ConfigDescription("Minimum number of active workers that must be available before activating the cluster")
    public NodeResourceStatusConfig setRequiredWorkersActive(int requiredWorkersActive)
    {
        this.requiredWorkersActive = requiredWorkersActive;
        return this;
    }
}
