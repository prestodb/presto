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

import javax.validation.constraints.Min;

public class DeterminismAnalyzerConfig
{
    private boolean runTeardown;
    private int maxAnalysisRuns = 2;
    private boolean handleLimitQuery = true;

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
}
