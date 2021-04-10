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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.LegacyConfig;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;

public class DeterminismAnalyzerConfig
{
    private boolean runTeardown;
    private int maxAnalysisRuns = 6;
    private boolean handleLimitQuery = true;
    private Set<String> nonDeterministicCatalogs = ImmutableSet.of();

    public boolean isRunTeardown()
    {
        return runTeardown;
    }

    @ConfigDescription("When set to false, temporary tables are not dropped for determinism analysis runs")
    @Config("determinism.run-teardown")
    @LegacyConfig("run-teardown-for-determinism-analysis")
    public DeterminismAnalyzerConfig setRunTeardown(boolean runTeardown)
    {
        this.runTeardown = runTeardown;
        return this;
    }

    @Min(0)
    public int getMaxAnalysisRuns()
    {
        return maxAnalysisRuns;
    }

    @Config("determinism.max-analysis-runs")
    @LegacyConfig("max-determinism-analysis-runs")
    public DeterminismAnalyzerConfig setMaxAnalysisRuns(int maxAnalysisRuns)
    {
        this.maxAnalysisRuns = maxAnalysisRuns;
        return this;
    }

    public boolean isHandleLimitQuery()
    {
        return handleLimitQuery;
    }

    @Config("determinism.handle-limit-query")
    @LegacyConfig("enable-limit-query-determinism-analyzer")
    public DeterminismAnalyzerConfig setHandleLimitQuery(boolean handleLimitQuery)
    {
        this.handleLimitQuery = handleLimitQuery;
        return this;
    }

    @NotNull
    public Set<String> getNonDeterministicCatalogs()
    {
        return nonDeterministicCatalogs;
    }

    @Config("determinism.non-deterministic-catalogs")
    @ConfigDescription("A comma-separated list of non-deterministic catalogs. Queries referencing table from those catalogs are treated as non-deterministic.")
    public DeterminismAnalyzerConfig setNonDeterministicCatalogs(String nonDeterministicCatalogs)
    {
        if (nonDeterministicCatalogs != null) {
            this.nonDeterministicCatalogs = Splitter.on(",").trimResults().splitToList(nonDeterministicCatalogs).stream()
                    .map(catalog -> catalog.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }
        return this;
    }
}
