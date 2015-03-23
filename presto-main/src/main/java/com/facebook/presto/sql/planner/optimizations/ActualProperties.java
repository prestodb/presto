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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableSet;

import java.util.List;

class ActualProperties
{
    // partitioning:
    //   partitioned: *, {k_i}
    //   unpartitioned

    // placement
    //   coordinator-only (=> unpartitioned)
    //   source
    //   anywhere

    private final PartitioningProperties partitioning;
    private final PlacementProperties placement;

    public ActualProperties(PartitioningProperties partitioning, PlacementProperties placement)
    {
        this.partitioning = partitioning;
        this.placement = placement;
    }

    public static ActualProperties of(PartitioningProperties partitioning, PlacementProperties placement)
    {
        return new ActualProperties(partitioning, placement);
    }

    public PartitioningProperties getPartitioning()
    {
        return partitioning;
    }

    public boolean isCoordinatorOnly()
    {
        return placement.getType() == PlacementProperties.Type.COORDINATOR_ONLY;
    }

    public boolean isPartitioned()
    {
        return partitioning.getType() == PartitioningProperties.Type.PARTITIONED;
    }

    public boolean isPartitionedOnKeys(List<Symbol> keys)
    {
        // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
        return isPartitioned() &&
                partitioning.getKeys().isPresent() &&
                ImmutableSet.copyOf(keys).containsAll(partitioning.getKeys().get());
    }

    public boolean isUnpartitioned()
    {
        return partitioning.getType() == PartitioningProperties.Type.UNPARTITIONED;
    }

    @Override
    public String toString()
    {
        return "partitioning: " + partitioning + ", placement: " + placement;
    }
}
