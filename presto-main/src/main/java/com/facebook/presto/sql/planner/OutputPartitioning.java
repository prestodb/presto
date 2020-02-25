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
package com.facebook.presto.sql.planner;

import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class OutputPartitioning
{
    private final PartitionFunction partitionFunction;
    private final List<Integer> partitionChannels;
    private final List<Optional<ConstantExpression>> partitionConstants;
    private final boolean replicateNullsAndAny;
    private final OptionalInt nullChannel;

    public OutputPartitioning(
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<ConstantExpression>> partitionConstants,
            boolean replicateNullsAndAny,
            OptionalInt nullChannel)
    {
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
        this.partitionConstants = ImmutableList.copyOf(requireNonNull(partitionConstants, "partitionConstants is null"));
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
    }

    public PartitionFunction getPartitionFunction()
    {
        return partitionFunction;
    }

    public List<Integer> getPartitionChannels()
    {
        return partitionChannels;
    }

    public List<Optional<ConstantExpression>> getPartitionConstants()
    {
        return partitionConstants;
    }

    public boolean isReplicateNullsAndAny()
    {
        return replicateNullsAndAny;
    }

    public OptionalInt getNullChannel()
    {
        return nullChannel;
    }
}
