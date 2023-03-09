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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.PartitioningSpillerFactory;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class LookupStarJoinOperators
{
    @Inject
    public LookupStarJoinOperators()
    {
    }

    public OperatorFactory innerJoin(int operatorId, PlanNodeId planNodeId, List<JoinBridgeManager<? extends LookupSourceFactory>> lookupSourceFactory, List<Type> probeTypes, List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels, OptionalInt totalOperatorsCount, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return createStarJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), LookupJoinOperators.JoinType.INNER, totalOperatorsCount, partitioningSpillerFactory);
    }

    public OperatorFactory probeOuterJoin(int operatorId, PlanNodeId planNodeId, List<JoinBridgeManager<? extends LookupSourceFactory>> lookupSourceFactory, List<Type> probeTypes, List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels, OptionalInt totalOperatorsCount, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return createStarJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), LookupJoinOperators.JoinType.PROBE_OUTER, totalOperatorsCount, partitioningSpillerFactory);
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private OperatorFactory createStarJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            List<JoinBridgeManager<? extends LookupSourceFactory>> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            List<Integer> probeOutputChannels,
            LookupJoinOperators.JoinType joinType,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
                .map(probeTypes::get)
                .collect(toImmutableList());
        List<Type> buildOutputChannelTypes = lookupSourceFactoryManager.stream()
                .flatMap(x -> x.getBuildOutputTypes().stream())
                .collect(toImmutableList());

        return new LookupStarJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactoryManager,
                probeTypes,
                probeOutputChannelTypes,
                buildOutputChannelTypes,
                joinType,
                new JoinProbeFactory(probeOutputChannels.stream().mapToInt(i -> i).toArray(), probeJoinChannel, probeHashChannel),
                totalOperatorsCount,
                probeJoinChannel,
                probeHashChannel,
                partitioningSpillerFactory);
    }
}
