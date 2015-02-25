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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestAggregationOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testAggregation()
            throws Exception
    {
        MetadataManager metadata = new MetadataManager();
        InternalAggregationFunction countVarcharColumn = metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR)), false).getAggregationFunction();
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, VARCHAR, BIGINT, DOUBLE, VARCHAR)
                .addSequencePage(100, 0, 0, 300, 500, 500, 500)
                .build();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0),
                        LONG_SUM.bind(ImmutableList.of(1), Optional.empty(), Optional.empty(), 1.0),
                        LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty(), Optional.empty(), 1.0),
                        VAR_BINARY_MAX.bind(ImmutableList.of(2), Optional.empty(), Optional.empty(), 1.0),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty(), Optional.empty(), 1.0),
                        DOUBLE_SUM.bind(ImmutableList.of(4), Optional.empty(), Optional.empty(), 1.0),
                        VAR_BINARY_MAX.bind(ImmutableList.of(5), Optional.empty(), Optional.empty(), 1.0)));
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR)
                .row(100, 4950, 49.5, "399", 100, 54950, 54950.0, "599")
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
