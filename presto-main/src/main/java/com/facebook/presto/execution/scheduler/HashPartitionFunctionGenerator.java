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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.HashPagePartitionFunction;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.NullPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashPartitionFunctionGenerator
        implements BiFunction<Integer, Integer, PagePartitionFunction>
{
    private final List<Integer> partitionChannels;
    private final Optional<Integer> hashChannel;
    private final List<Type> types;
    private final NullPartitioning nullPartitioning;

    public HashPartitionFunctionGenerator(PlanFragment fragment)
    {
        requireNonNull(fragment, "fragment is null");
        checkState(fragment.getOutputPartitioning() == OutputPartitioning.HASH, "fragment is not hash partitioned");

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        partitionChannels = fragment.getPartitionBy().get().stream()
                .map(symbol -> fragment.getOutputLayout().indexOf(symbol))
                .collect(toImmutableList());
        hashChannel = fragment.getHash().map(fragment.getOutputLayout()::indexOf);

        types = fragment.getTypes();

        nullPartitioning = fragment.getNullPartitionPolicy().get();
    }

    public HashPartitionFunctionGenerator(List<Integer> partitionChannels, Optional<Integer> hashChannel, List<Type> types, NullPartitioning nullPartitioning)
    {
        this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.nullPartitioning = requireNonNull(nullPartitioning, "nullPartitioning is null");
    }

    @Override
    public PagePartitionFunction apply(Integer partition, Integer partitionCount)
    {
        return new HashPagePartitionFunction(partition, partitionCount, partitionChannels, hashChannel, types, nullPartitioning);
    }
}
