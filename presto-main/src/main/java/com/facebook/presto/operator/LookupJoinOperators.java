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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinProbeCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class LookupJoinOperators
{
    public enum JoinType
    {
        INNER,
        PROBE_OUTER, // the Probe is the outer side of the join
        LOOKUP_OUTER, // The LookupSource is the outer side of the join
        FULL_OUTER,
    }

    private LookupJoinOperators()
    {
    }

    private static final JoinProbeCompiler JOIN_PROBE_COMPILER = new JoinProbeCompiler();

    public static OperatorFactory innerJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, Optional<List<Integer>> probeOutputChannels)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.INNER);
    }

    public static OperatorFactory probeOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, Optional<List<Integer>> probeOutputChannels)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.PROBE_OUTER);
    }

    public static OperatorFactory lookupOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, Optional<List<Integer>> probeOutputChannels)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.LOOKUP_OUTER);
    }

    public static OperatorFactory fullOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, Optional<List<Integer>> probeOutputChannels)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.FULL_OUTER);
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }
}
