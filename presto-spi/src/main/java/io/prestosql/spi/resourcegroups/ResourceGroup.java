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
package io.prestosql.spi.resourcegroups;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

public interface ResourceGroup
{
    ResourceGroupId getId();

    DataSize getSoftMemoryLimit();

    /**
     * Threshold on total distributed memory usage after which new queries
     * will queue instead of starting.
     */
    void setSoftMemoryLimit(DataSize limit);

    Duration getSoftCpuLimit();

    /**
     * Threshold on total distributed CPU usage after which max running queries will be reduced.
     */
    void setSoftCpuLimit(Duration limit);

    Duration getHardCpuLimit();

    /**
     * Threshold on total distributed CPU usage after which new queries
     * will queue instead of starting.
     */
    void setHardCpuLimit(Duration limit);

    long getCpuQuotaGenerationMillisPerSecond();

    /**
     * Rate at which distributed CPU usage quota regenerates.
     */
    void setCpuQuotaGenerationMillisPerSecond(long rate);

    int getSoftConcurrencyLimit();

    /**
     * Number of concurrently running queries after which new queries will only run if
     * all peer resource groups below their soft limits are ineligible or if all
     * eligible peers are above soft limits.
     */
    void setSoftConcurrencyLimit(int softConcurrencyLimit);

    int getHardConcurrencyLimit();

    /**
     * Maximum number of concurrently running queries, after which
     * new queries will queue instead of starting.
     */
    void setHardConcurrencyLimit(int hardConcurrencyLimit);

    int getMaxQueuedQueries();

    /**
     * Maximum number of queued queries after which submitted queries will be rejected.
     */
    void setMaxQueuedQueries(int maxQueuedQueries);

    int getSchedulingWeight();

    /**
     * Scheduling weight of this group in its parent group.
     */
    void setSchedulingWeight(int weight);

    SchedulingPolicy getSchedulingPolicy();

    /**
     * Scheduling policy to use when dividing resources among child resource groups,
     * or among queries submitted to this group.
     */
    void setSchedulingPolicy(SchedulingPolicy policy);

    boolean getJmxExport();

    /**
     * Whether to export statistics about this group and allow configuration via JMX.
     */
    void setJmxExport(boolean export);
}
