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
package com.facebook.presto.resourceGroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalTime;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class SelectorTimeOfDay
{
    private final LocalTime startTime;
    private final LocalTime endTime;

    @JsonCreator
    public SelectorTimeOfDay(
            @JsonProperty("startTime") String startTime,
            @JsonProperty("endTime") String endTime)
    {
        requireNonNull(startTime, "startTime is null");
        requireNonNull(endTime, "endTime is null");

        this.startTime = LocalTime.parse(startTime);
        this.endTime = LocalTime.parse(endTime);
    }

    @JsonProperty
    public LocalTime getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public LocalTime getEndTime()
    {
        return endTime;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof SelectorTimeOfDay)) {
            return false;
        }
        else {
            SelectorTimeOfDay that = (SelectorTimeOfDay) other;
            return this.startTime == that.startTime && this.endTime == that.endTime;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                startTime,
                endTime);
    }
}
