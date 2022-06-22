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
package com.facebook.presto.cost;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.statistics.EmptyPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.ExternalPlanStatisticsProvider;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsManager
{
    private ExternalPlanStatisticsProvider externalPlanStatisticsProvider;
    private final Metadata metadata;
    private boolean externalProviderAdded;

    @Inject
    public HistoryBasedPlanStatisticsManager(Metadata metadata)
    {
        this.externalPlanStatisticsProvider = EmptyPlanStatisticsProvider.getInstance();
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.externalProviderAdded = false;
    }

    public void addExternalPlanStatisticsProviderFactory(ExternalPlanStatisticsProvider externalPlanStatisticsProvider)
    {
        if (externalProviderAdded) {
            throw new IllegalStateException("externalPlanStatisticsProviderFactory can only be set once");
        }
        this.externalPlanStatisticsProvider = externalPlanStatisticsProvider;
        externalProviderAdded = true;
    }

    public HistoryBasedPlanStatisticsCalculator getHistoryBasedPlanStatisticsCalculator(StatsCalculator delegate)
    {
        return new HistoryBasedPlanStatisticsCalculator(() -> externalPlanStatisticsProvider, metadata, delegate);
    }
}
