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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.ExchangeEncoding;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.NATIVE_MIN_COLUMNAR_ENCODING_CHANNELS_TO_PREFER_ROW_WISE_ENCODING;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.plan.ExchangeEncoding.ROW_WISE;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineRemotePartitionedExchangeEncoding.estimateNumberOfColumnarChannels;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineRemotePartitionedExchangeEncoding.estimateNumberOfOutputColumnarChannels;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestDetermineRemotePartitionedExchangeEncoding
{
    private static final int MIN_COLUMNAR_STREAMS = 100;

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testPrestoOnSpark()
    {
        // special exchanges are always columnar
        assertForPrestoOnSpark()
                .on(p -> createExchange(FIXED_ARBITRARY_DISTRIBUTION, MIN_COLUMNAR_STREAMS))
                .doesNotFire();
        // do not fire twice
        assertForPrestoOnSpark()
                .on(p -> createExchange(FIXED_HASH_DISTRIBUTION, MIN_COLUMNAR_STREAMS).withRowWiseEncoding())
                .doesNotFire();
        // hash based exchanges are always row wise in Presto on Spark
        assertForPrestoOnSpark()
                .on(p -> createExchange(FIXED_HASH_DISTRIBUTION, MIN_COLUMNAR_STREAMS - 1))
                .matches(exchangeEncoding(ROW_WISE));
    }

    @Test
    public void testPresto()
    {
        // exchanges are always columnar in Presto
        assertForPresto()
                .on(p -> createExchange(FIXED_ARBITRARY_DISTRIBUTION, MIN_COLUMNAR_STREAMS))
                .doesNotFire();
        assertForPresto()
                .on(p -> createExchange(FIXED_HASH_DISTRIBUTION, MIN_COLUMNAR_STREAMS - 1))
                .doesNotFire();
        assertForPresto()
                .on(p -> createExchange(FIXED_HASH_DISTRIBUTION, MIN_COLUMNAR_STREAMS + 1))
                .doesNotFire();
    }

    @Test
    public void testNative()
    {
        // special exchanges are always columnar
        assertForNative()
                .on(p -> createExchange(FIXED_ARBITRARY_DISTRIBUTION, MIN_COLUMNAR_STREAMS))
                .doesNotFire();
        // hash based exchange with the total number of output columnar streams lower than threshold is columnar
        assertForNative()
                .on(p -> createExchange(FIXED_HASH_DISTRIBUTION, MIN_COLUMNAR_STREAMS - 1))
                .doesNotFire();
        // otherwise row wise
        assertForNative()
                .on(p -> createExchange(FIXED_HASH_DISTRIBUTION, MIN_COLUMNAR_STREAMS))
                .matches(exchangeEncoding(ROW_WISE));
    }

    private RuleAssert assertForPrestoOnSpark()
    {
        return createAssert(false, true);
    }

    private RuleAssert assertForNative()
    {
        return createAssert(true, false);
    }

    private RuleAssert assertForPresto()
    {
        return createAssert(false, false);
    }

    private RuleAssert createAssert(boolean nativeExecution, boolean prestoSparkExecutionEnvironment)
    {
        return tester.assertThat(new DetermineRemotePartitionedExchangeEncoding(nativeExecution, prestoSparkExecutionEnvironment))
                .setSystemProperty(NATIVE_MIN_COLUMNAR_ENCODING_CHANNELS_TO_PREFER_ROW_WISE_ENCODING, MIN_COLUMNAR_STREAMS + "");
    }

    private static ExchangeNode createExchange(PartitioningHandle handle, int numberOfOutputColumnarStreams)
    {
        int numberOfBigintColumns = numberOfOutputColumnarStreams / 2;
        List<Type> types = IntStream.range(0, numberOfBigintColumns)
                .mapToObj(i -> BIGINT)
                .collect(toImmutableList());
        ExchangeNode exchangeNode = createExchangeNode(handle, types, types);
        assertEquals(estimateNumberOfOutputColumnarChannels(exchangeNode), numberOfBigintColumns * 2);
        return exchangeNode;
    }

    private static PlanMatchPattern exchangeEncoding(ExchangeEncoding encoding)
    {
        return node(ExchangeNode.class, node(ValuesNode.class)).with(new ExchangeEncodingMatcher(encoding));
    }

    @Test
    public void testEstimateNumberOfOutputColumnarChannels()
    {
        assertEquals(estimateNumberOfOutputColumnarChannels(createExchangeNode(FIXED_HASH_DISTRIBUTION, ImmutableList.of(BIGINT), ImmutableList.of(BIGINT))), 2);
        assertEquals(estimateNumberOfOutputColumnarChannels(createExchangeNode(FIXED_HASH_DISTRIBUTION, ImmutableList.of(BIGINT, VARCHAR), ImmutableList.of(BIGINT))), 2);
    }

    private static ExchangeNode createExchangeNode(PartitioningHandle handle, List<Type> inputTypes, List<Type> outputTypes)
    {
        return partitionedExchange(
                new PlanNodeId("exchange"),
                REMOTE_STREAMING,
                new ValuesNode(
                        Optional.empty(),
                        new PlanNodeId("values"),
                        createExpressions(inputTypes),
                        ImmutableList.of(),
                        Optional.empty()),
                new PartitioningScheme(
                        Partitioning.create(handle, ImmutableList.of()),
                        createExpressions(outputTypes)));
    }

    private static List<VariableReferenceExpression> createExpressions(List<Type> types)
    {
        ImmutableList.Builder<VariableReferenceExpression> result = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            result.add(new VariableReferenceExpression(Optional.empty(), "exp_" + i, types.get(i)));
        }
        return result.build();
    }

    @Test
    public void testEstimateNumberOfColumnarChannels()
    {
        assertEquals(estimateNumberOfColumnarChannels(BIGINT), 2);
        assertEquals(estimateNumberOfColumnarChannels(REAL), 2);
        assertEquals(estimateNumberOfColumnarChannels(VARCHAR), 3);
        assertEquals(estimateNumberOfColumnarChannels(createVarcharType(10)), 3);
        assertEquals(estimateNumberOfColumnarChannels(createDecimalType(3, 2)), 2);
        assertEquals(estimateNumberOfColumnarChannels(createDecimalType(30, 2)), 2);
        assertEquals(estimateNumberOfColumnarChannels(new ArrayType(BIGINT)), 4);
        assertEquals(estimateNumberOfColumnarChannels(new ArrayType(VARCHAR)), 5);
        assertEquals(estimateNumberOfColumnarChannels(RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR))), 7);
    }

    private static class ExchangeEncodingMatcher
            implements Matcher
    {
        private final ExchangeEncoding encoding;

        private ExchangeEncodingMatcher(ExchangeEncoding encoding)
        {
            this.encoding = requireNonNull(encoding, "encoding is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof ExchangeNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            ExchangeNode exchangeNode = (ExchangeNode) node;
            return exchangeNode.getPartitioningScheme().getEncoding() == encoding ? match() : NO_MATCH;
        }
    }
}
