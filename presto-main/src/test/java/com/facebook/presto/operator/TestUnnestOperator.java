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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestUnnestOperator
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
    public void testUnnest()
            throws Exception
    {
        MetadataManager metadata = new MetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array<bigint>"));
        Type mapType = metadata.getType(parseTypeSignature("map<bigint,bigint>"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1, "[2, 3]", "{\"4\": 5}")
                .row(2, "[99]", null)
                .row(3, null, null)
                .pageBreak()
                .row(6, "[7, 8]", "{\"9\": 10, \"11\": 12}")
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(0, ImmutableList.of(0), ImmutableList.<Type>of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType));
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1, 2, 4, 5)
                .row(1, 3, null, null)
                .row(2, 99, null, null)
                .row(6, 7, 9, 10)
                .row(6, 8, 11, 12)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testUnnestNonNumericDoubles()
            throws Exception
    {
        MetadataManager metadata = new MetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array<double>"));
        Type mapType = metadata.getType(parseTypeSignature("map<bigint,double>"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1, "[\"-Infinity\", \"Infinity\", \"NaN\"]", "{\"1\": \"-Infinity\", \"2\": \"Infinity\", \"3\": \"NaN\"}")
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(0, ImmutableList.of(0), ImmutableList.<Type>of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType));
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT, DOUBLE)
                .row(1, NEGATIVE_INFINITY, 1, NEGATIVE_INFINITY)
                .row(1, POSITIVE_INFINITY, 2, POSITIVE_INFINITY)
                .row(1, NaN, 3, NaN)
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
