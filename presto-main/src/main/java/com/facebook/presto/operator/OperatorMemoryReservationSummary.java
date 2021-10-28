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

import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OperatorMemoryReservationSummary
{
    private final String type;
    private final PlanNodeId planNodeId;
    private final List<DataSize> reservations;
    private final DataSize total;
    private final Optional<String> info;

    @JsonCreator
    public OperatorMemoryReservationSummary(
            @JsonProperty("type") String type,
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("reservations") List<DataSize> reservations,
            @JsonProperty("total") DataSize total,
            @JsonProperty("info") Optional<String> info)
    {
        this.type = requireNonNull(type, "type is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.reservations = ImmutableList.copyOf(requireNonNull(reservations, "reservations is null"));
        this.total = requireNonNull(total, "total is null");
        this.info = requireNonNull(info, "info is null");
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public List<DataSize> getReservations()
    {
        return reservations;
    }

    @JsonProperty
    public DataSize getTotal()
    {
        return total;
    }

    @JsonProperty
    public Optional<String> getInfo()
    {
        return info;
    }
}
