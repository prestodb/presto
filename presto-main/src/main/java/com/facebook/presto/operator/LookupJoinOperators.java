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

public class LookupJoinOperators
{
    public enum JoinType {
        INNER,
        PROBE_OUTER, // the Probe is the outer side of the join
        LOOKUP_OUTER, // The LookupSource is the outer side of the join
        FULL_OUTER,
    }

    private LookupJoinOperators()
    {
    }

    private static final JoinProbeCompiler JOIN_PROBE_COMPILER = new JoinProbeCompiler();

    public static OperatorFactory innerJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, JoinType.INNER, filterFunctionPresent);
    }

    public static OperatorFactory probeOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, JoinType.PROBE_OUTER, filterFunctionPresent);
    }

    public static OperatorFactory lookupOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, JoinType.LOOKUP_OUTER, filterFunctionPresent);
    }

    public static OperatorFactory fullOuterJoin(int operatorId, PlanNodeId planNodeId, LookupSourceFactory lookupSourceFactory, List<? extends Type> probeTypes, List<Integer> probeJoinChannel, Optional<Integer> probeHashChannel, boolean filterFunctionPresent)
    {
        return JOIN_PROBE_COMPILER.compileJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, JoinType.FULL_OUTER, filterFunctionPresent);
    }
}
