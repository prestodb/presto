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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;
import com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

@Immutable
@EventType("DeterminismAnalysisDetails")
public class DeterminismAnalysisDetails
{
    private final List<DeterminismAnalysisRun> runs;
    private final String limitQueryAnalysis;
    private final String limitQueryAnalysisQueryId;

    @JsonCreator
    public DeterminismAnalysisDetails(
            List<DeterminismAnalysisRun> runs,
            LimitQueryDeterminismAnalysis limitQueryAnalysis,
            Optional<String> limitQueryAnalysisQueryId)
    {
        this.runs = ImmutableList.copyOf(runs);
        this.limitQueryAnalysis = limitQueryAnalysis.name();
        this.limitQueryAnalysisQueryId = limitQueryAnalysisQueryId.orElse(null);
    }

    @EventField
    public List<DeterminismAnalysisRun> getRuns()
    {
        return runs;
    }

    @EventField
    public String getLimitQueryAnalysis()
    {
        return limitQueryAnalysis;
    }

    @EventField
    public String getLimitQueryAnalysisQueryId()
    {
        return limitQueryAnalysisQueryId;
    }
}
