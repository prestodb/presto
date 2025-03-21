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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel;

import java.util.Optional;

import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.isBelowMaxBroadcastSize;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.mustPartition;

public class ConfidenceBasedBroadcastUtil
{
    private ConfidenceBasedBroadcastUtil() {};

    public static Optional<JoinNode> confidenceBasedBroadcast(JoinNode joinNode, Rule.Context context)
    {
        ConfidenceLevel rightConfidence = context.getStatsProvider().getStats(joinNode.getRight()).confidenceLevel();
        ConfidenceLevel leftConfidence = context.getStatsProvider().getStats(joinNode.getLeft()).confidenceLevel();

        if (rightConfidence.getConfidenceOrdinal() > leftConfidence.getConfidenceOrdinal()) {
            return Optional.of(joinNode.withDistributionType(REPLICATED));
        }
        else if (leftConfidence.getConfidenceOrdinal() > rightConfidence.getConfidenceOrdinal()) {
            return Optional.of(joinNode.flipChildren().withDistributionType(REPLICATED));
        }

        return Optional.empty();
    }

    public static Optional<JoinNode> treatLowConfidenceZeroEstimationsAsUnknown(boolean probeSideLowConfidenceZero, boolean buildSideLowConfidenceZero, JoinNode joinNode, Rule.Context context)
    {
        if (buildSideLowConfidenceZero && probeSideLowConfidenceZero) {
            return Optional.of(joinNode.withDistributionType(PARTITIONED));
        }
        else if (buildSideLowConfidenceZero) {
            if (isBelowMaxBroadcastSize(joinNode.flipChildren(), context) && !mustPartition(joinNode)) {
                return Optional.of(joinNode.flipChildren().withDistributionType(REPLICATED));
            }
            else {
                return Optional.of(joinNode.withDistributionType(PARTITIONED));
            }
        }
        else if (probeSideLowConfidenceZero) {
            if (isBelowMaxBroadcastSize(joinNode, context) && !mustPartition(joinNode)) {
                return Optional.of(joinNode.withDistributionType(REPLICATED));
            }
            else {
                return Optional.of(joinNode.withDistributionType(PARTITIONED));
            }
        }
        else {
            return Optional.empty();
        }
    }
}
