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
import com.facebook.presto.verifier.framework.DeterminismAnalysis;
import com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.LimitQueryDeterminismAnalysis.NOT_RUN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
@EventType("DeterminismAnalysisDetails")
public class DeterminismAnalysisDetails
{
    private final DeterminismAnalysis determinismAnalysis;
    private final List<DeterminismAnalysisRun> runs;
    private final String limitQueryAnalysis;
    private final String limitQueryAnalysisQueryId;

    @JsonCreator
    public DeterminismAnalysisDetails(
            DeterminismAnalysis determinismAnalysis,
            List<DeterminismAnalysisRun> runs,
            LimitQueryDeterminismAnalysis limitQueryAnalysis,
            Optional<String> limitQueryAnalysisQueryId)
    {
        this.determinismAnalysis = requireNonNull(determinismAnalysis, "determinismAnalysis is null");
        this.runs = ImmutableList.copyOf(runs);
        this.limitQueryAnalysis = limitQueryAnalysis.name();
        this.limitQueryAnalysisQueryId = limitQueryAnalysisQueryId.orElse(null);
    }

    public DeterminismAnalysis getDeterminismAnalysis()
    {
        return determinismAnalysis;
    }

    @EventField
    public String getResult()
    {
        return determinismAnalysis.name();
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private List<DeterminismAnalysisRun.Builder> runs = new ArrayList<>();
        private Optional<LimitQueryDeterminismAnalysis> limitQueryAnalysis = Optional.empty();
        private Optional<String> limitQueryAnalysisQueryId = Optional.empty();

        private Builder()
        {
        }

        public DeterminismAnalysisRun.Builder addRun()
        {
            DeterminismAnalysisRun.Builder run = DeterminismAnalysisRun.builder();
            runs.add(run);
            return run;
        }

        public void setLimitQueryAnalysis(LimitQueryDeterminismAnalysis limitQueryAnalysis)
        {
            checkState(!this.limitQueryAnalysis.isPresent(), "limitQueryAnalysis is already set");
            this.limitQueryAnalysis = Optional.of(limitQueryAnalysis);
        }

        public void setLimitQueryAnalysisQueryId(String limitQueryAnalysisQueryId)
        {
            checkState(!this.limitQueryAnalysisQueryId.isPresent(), "limitQueryAnalysis is already set");
            this.limitQueryAnalysisQueryId = Optional.of(limitQueryAnalysisQueryId);
        }

        public DeterminismAnalysisDetails build(DeterminismAnalysis analysis)
        {
            return new DeterminismAnalysisDetails(
                    analysis,
                    runs.stream()
                            .map(DeterminismAnalysisRun.Builder::build)
                            .collect(toImmutableList()),
                    limitQueryAnalysis.orElse(NOT_RUN),
                    limitQueryAnalysisQueryId);
        }
    }
}
