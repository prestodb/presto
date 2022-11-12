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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedList;
import java.util.List;

@ThreadSafe
public class OptimizerInformationCollector
{
    @GuardedBy("this")
    private final List<PlanOptimizerInformation> optimizationInfo = new LinkedList<>();

    public synchronized void addInformation(PlanOptimizerInformation optimizerInformation)
    {
        this.optimizationInfo.add(optimizerInformation);
    }

    public synchronized List<PlanOptimizerInformation> getOptimizationInfo()
    {
        return ImmutableList.copyOf(optimizationInfo);
    }
}
