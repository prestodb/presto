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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.facebook.presto.operator.JoinStatisticsCounter.HISTOGRAM_BUCKETS;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class JoinOperatorInfo
        implements Mergeable<JoinOperatorInfo>, OperatorInfo
{
    private final JoinType joinType;
    private final long[] logHistogramProbes;
    private final long[] logHistogramOutput;
    private final long lookupSourcePositions;

    public static JoinOperatorInfo createJoinOperatorInfo(JoinType joinType, long[] logHistogramCounters, long lookupSourcePositions)
    {
        long[] logHistogramProbes = new long[HISTOGRAM_BUCKETS];
        long[] logHistogramOutput = new long[HISTOGRAM_BUCKETS];
        for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
            logHistogramProbes[i] = logHistogramCounters[2 * i];
            logHistogramOutput[i] = logHistogramCounters[2 * i + 1];
        }
        return new JoinOperatorInfo(joinType, logHistogramProbes, logHistogramOutput, lookupSourcePositions);
    }

    @JsonCreator
    public JoinOperatorInfo(
            @JsonProperty("joinType") JoinType joinType,
            @JsonProperty("logHistogramProbes") long[] logHistogramProbes,
            @JsonProperty("logHistogramOutput") long[] logHistogramOutput,
            @JsonProperty("lookupSourcePositions") long lookupSourcePositions)
    {
        checkArgument(logHistogramProbes.length == HISTOGRAM_BUCKETS);
        checkArgument(logHistogramOutput.length == HISTOGRAM_BUCKETS);
        this.joinType = joinType;
        this.logHistogramProbes = logHistogramProbes;
        this.logHistogramOutput = logHistogramOutput;
        this.lookupSourcePositions = lookupSourcePositions;
    }

    @JsonProperty
    public JoinType getJoinType()
    {
        return joinType;
    }

    @JsonProperty
    public long[] getLogHistogramProbes()
    {
        return logHistogramProbes;
    }

    @JsonProperty
    public long[] getLogHistogramOutput()
    {
        return logHistogramOutput;
    }

    @JsonProperty
    public long getLookupSourcePositions()
    {
        return lookupSourcePositions;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("joinType", joinType)
                .add("logHistogramProbes", logHistogramProbes)
                .add("logHistogramOutput", logHistogramOutput)
                .add("lookupSourcePositions", lookupSourcePositions)
                .toString();
    }

    @Override
    public JoinOperatorInfo mergeWith(JoinOperatorInfo other)
    {
        checkState(this.joinType.equals(other.joinType), "different join types");
        long[] logHistogramProbes = new long[HISTOGRAM_BUCKETS];
        long[] logHistogramOutput = new long[HISTOGRAM_BUCKETS];
        for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
            logHistogramProbes[i] = this.logHistogramProbes[i] + other.logHistogramProbes[i];
            logHistogramOutput[i] = this.logHistogramOutput[i] + other.logHistogramOutput[i];
        }
        return new JoinOperatorInfo(this.joinType, logHistogramProbes, logHistogramOutput, this.lookupSourcePositions + other.lookupSourcePositions);
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }
}
