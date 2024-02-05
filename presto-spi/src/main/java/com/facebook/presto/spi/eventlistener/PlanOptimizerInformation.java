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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PlanOptimizerInformation
{
    // Name of the optimizer, we use the class name of the optimizer here.
    private final String optimizerName;
    // True when the optimizer makes changes to query plan, otherwise false
    private final boolean optimizerTriggered;
    // For optimizers which are not enabled. True if the query matches the pattern of the optimizer and could be applied.
    // False if cannot be applied. Empty if information not available.
    private final Optional<Boolean> optimizerApplicable;
    // True if optimizer encounter failures (for example timeout etc.), false if no failures, empty if information not available.
    private final Optional<Boolean> optimizerFailure;
    private final Optional<Boolean> isCostBased;
    private final Optional<String> statsSource;

    @JsonCreator
    public PlanOptimizerInformation(
            @JsonProperty("optimizerName") String optimizerName,
            @JsonProperty("optimizerTriggered") boolean optimizerTriggered,
            @JsonProperty("optimizerApplicable") Optional<Boolean> optimizerApplicable,
            @JsonProperty("optimizerFailure") Optional<Boolean> optimizerFailure,
            @JsonProperty("isCostBased") Optional<Boolean> isCostBased,
            @JsonProperty("statsSource") Optional<String> statsSource)
    {
        this.optimizerName = requireNonNull(optimizerName, "optimizerName is null");
        this.optimizerTriggered = requireNonNull(optimizerTriggered, "optimizerTriggered is null");
        this.optimizerApplicable = requireNonNull(optimizerApplicable, "optimizerApplicable is null");
        this.optimizerFailure = requireNonNull(optimizerFailure, "optimizerFailure is null");
        this.isCostBased = requireNonNull(isCostBased, "isCostBased is null");
        this.statsSource = requireNonNull(statsSource, "statsSource is null");
    }

    @JsonProperty
    public String getOptimizerName()
    {
        return optimizerName;
    }

    @JsonProperty
    public boolean getOptimizerTriggered()
    {
        return optimizerTriggered;
    }

    @JsonProperty
    public Optional<Boolean> getOptimizerApplicable()
    {
        return optimizerApplicable;
    }

    @JsonProperty
    public Optional<Boolean> getOptimizerFailure()
    {
        return optimizerFailure;
    }

    @JsonProperty
    public Optional<Boolean> getIsCostBased()
    {
        return isCostBased;
    }

    @JsonProperty
    public Optional<String> getStatsSource()
    {
        return statsSource;
    }
}
