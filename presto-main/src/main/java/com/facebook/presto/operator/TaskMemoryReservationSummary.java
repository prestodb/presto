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
package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TaskMemoryReservationSummary
{
    private final String taskId;
    private final DataSize reservation;
    private final List<OperatorMemoryReservationSummary> topConsumers;

    @JsonCreator
    public TaskMemoryReservationSummary(
            @JsonProperty("taskId") String taskId,
            @JsonProperty("reservation") DataSize reservation,
            @JsonProperty("topConsumers") List<OperatorMemoryReservationSummary> topConsumers)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.reservation = requireNonNull(reservation, "reservation is null");
        this.topConsumers = ImmutableList.copyOf(requireNonNull(topConsumers, "topConsumers is null"));
    }

    @JsonProperty
    public String getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public DataSize getReservation()
    {
        return reservation;
    }

    @JsonProperty
    public List<OperatorMemoryReservationSummary> getTopConsumers()
    {
        return topConsumers;
    }
}
