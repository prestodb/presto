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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.annotations.VisibleForTesting;

import static com.facebook.presto.SystemSessionProperties.getMinColumnarEncodingChannelsToPreferRowWiseEncoding;
import static com.facebook.presto.spi.plan.ExchangeEncoding.ROW_WISE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.Exchange.scope;
import static com.facebook.presto.sql.planner.plan.Patterns.Exchange.type;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;

public class DetermineRemotePartitionedExchangeEncoding
        implements Rule<ExchangeNode>
{
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .with(scope().equalTo(REMOTE_STREAMING))
            .with(type().equalTo(REPARTITION));

    private final boolean nativeExecution;
    private final boolean prestoSparkExecutionEnvironment;

    public DetermineRemotePartitionedExchangeEncoding(boolean nativeExecution, boolean prestoSparkExecutionEnvironment)
    {
        this.nativeExecution = nativeExecution;
        this.prestoSparkExecutionEnvironment = prestoSparkExecutionEnvironment;
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return nativeExecution || prestoSparkExecutionEnvironment;
    }

    @Override
    public Result apply(ExchangeNode node, Captures captures, Context context)
    {
        if (prestoSparkExecutionEnvironment) {
            // In Presto on Spark, row-wise encoding is always used for non-special shuffles (i.e., excluding broadcast, single, and arbitrary shuffles).
            // To accurately reflect this in the plan, the exchange encoding is set here.
            // Presto on Spark does not check the ExchangeEncoding specified in the plan.
            return determineForPrestoOnSpark(node);
        }
        if (nativeExecution) {
            return determineForNativeExecution(context.getSession(), node);
        }
        // Presto Java runtime does not support row-wise encoding
        return Result.empty();
    }

    private Result determineForPrestoOnSpark(ExchangeNode node)
    {
        // keep columnar for special cases
        if (node.getPartitioningScheme().isSingleOrBroadcastOrArbitrary()) {
            return Result.empty();
        }
        if (node.getPartitioningScheme().getEncoding() == ROW_WISE) {
            // leave untouched if already visited
            return Result.empty();
        }
        // otherwise switch to row-wise
        return Result.ofPlanNode(node.withRowWiseEncoding());
    }

    private Result determineForNativeExecution(Session session, ExchangeNode node)
    {
        // keep columnar for special cases
        if (node.getPartitioningScheme().isSingleOrBroadcastOrArbitrary()) {
            return Result.empty();
        }
        if (node.getPartitioningScheme().getEncoding() == ROW_WISE) {
            // leave untouched if already visited
            return Result.empty();
        }
        int minChannelsToPreferRowWiseEncoding = getMinColumnarEncodingChannelsToPreferRowWiseEncoding(session);
        if (estimateNumberOfOutputColumnarChannels(node) >= minChannelsToPreferRowWiseEncoding) {
            return Result.ofPlanNode(node.withRowWiseEncoding());
        }
        return Result.empty();
    }

    @VisibleForTesting
    static long estimateNumberOfOutputColumnarChannels(ExchangeNode node)
    {
        return node.getOutputVariables().stream()
                .map(VariableReferenceExpression::getType)
                .mapToLong(DetermineRemotePartitionedExchangeEncoding::estimateNumberOfColumnarChannels)
                .sum();
    }

    @VisibleForTesting
    static long estimateNumberOfColumnarChannels(Type type)
    {
        if (type instanceof FixedWidthType) {
            // nulls and values
            return 2;
        }
        if (!type.getTypeParameters().isEmpty()) {
            // complex type
            // nulls and offsets
            long result = 2;
            for (Type parameter : type.getTypeParameters()) {
                result += estimateNumberOfColumnarChannels(parameter);
            }
            return result;
        }
        // nulls, offsets, values
        return 3;
    }
}
