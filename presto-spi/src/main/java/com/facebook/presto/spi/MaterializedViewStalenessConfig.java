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
package com.facebook.presto.spi;

import com.facebook.airlift.units.Duration;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public final class MaterializedViewStalenessConfig
{
    private final MaterializedViewStaleReadBehavior staleReadBehavior;
    private final Duration stalenessWindow;

    @JsonCreator
    public MaterializedViewStalenessConfig(
            @JsonProperty("staleReadBehavior") MaterializedViewStaleReadBehavior staleReadBehavior,
            @JsonProperty("stalenessWindow") Duration stalenessWindow)
    {
        this.staleReadBehavior = requireNonNull(staleReadBehavior, "staleReadBehavior is null");
        this.stalenessWindow = requireNonNull(stalenessWindow, "stalenessWindow is null");
    }

    @JsonProperty
    public MaterializedViewStaleReadBehavior getStaleReadBehavior()
    {
        return staleReadBehavior;
    }

    @JsonProperty
    public Duration getStalenessWindow()
    {
        return stalenessWindow;
    }

    @Override
    public String toString()
    {
        return "MaterializedViewStalenessConfig{" +
                "staleReadBehavior=" + staleReadBehavior +
                ", stalenessWindow=" + stalenessWindow +
                '}';
    }
}
