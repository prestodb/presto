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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RowType.withDefaultFieldNames;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.buildExpectedPage;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.buildOutputTypes;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.calculateMaxCardinalities;
import static com.facebook.presto.operator.unnest.TestUnnesterUtil.mergePages;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestUnnestOperator
{
    private static final int PAGE_COUNT = 10;
    private static final int POSITION_COUNT = 500;

    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
    private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testUnnest()
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array(bigint)"));
        Type mapType = metadata.getType(parseTypeSignature("map(bigint,bigint)"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1L, arrayBlockOf(BIGINT, 2, 3), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(4, 5)))
                .row(2L, arrayBlockOf(BIGINT, 99), null)
                .row(3L, null, null)
                .pageBreak()
                .row(6L, arrayBlockOf(BIGINT, 7, 8), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(9, 10, 11, 12)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 2L, 4L, 5L)
                .row(1L, 3L, null, null)
                .row(2L, 99L, null, null)
                .row(6L, 7L, 9L, 10L)
                .row(6L, 8L, 11L, 12L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestWithArray()
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array(array(bigint))"));
        Type mapType = metadata.getType(parseTypeSignature("map(array(bigint),array(bigint))"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(
                        1L,
                        arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(2, 4), ImmutableList.of(3, 6)),
                        mapBlockOf(new ArrayType(BIGINT), new ArrayType(BIGINT), ImmutableMap.of(ImmutableList.of(4, 8), ImmutableList.of(5, 10))))
                .row(2L, arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(99, 198)), null)
                .row(3L, null, null)
                .pageBreak()
                .row(
                        6,
                        arrayBlockOf(new ArrayType(BIGINT), ImmutableList.of(7, 14), ImmutableList.of(8, 16)),
                        mapBlockOf(new ArrayType(BIGINT), new ArrayType(BIGINT), ImmutableMap.of(ImmutableList.of(9, 18), ImmutableList.of(10, 20), ImmutableList.of(11, 22), ImmutableList.of(12, 24))))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, new ArrayType(BIGINT), new ArrayType(BIGINT), new ArrayType(BIGINT))
                .row(1L, ImmutableList.of(2L, 4L), ImmutableList.of(4L, 8L), ImmutableList.of(5L, 10L))
                .row(1L, ImmutableList.of(3L, 6L), null, null)
                .row(2L, ImmutableList.of(99L, 198L), null, null)
                .row(6L, ImmutableList.of(7L, 14L), ImmutableList.of(9L, 18L), ImmutableList.of(10L, 20L))
                .row(6L, ImmutableList.of(8L, 16L), ImmutableList.of(11L, 22L), ImmutableList.of(12L, 24L))
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestWithOrdinality()
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array(bigint)"));
        Type mapType = metadata.getType(parseTypeSignature("map(bigint,bigint)"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1L, arrayBlockOf(BIGINT, 2, 3), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(4, 5)))
                .row(2L, arrayBlockOf(BIGINT, 99), null)
                .row(3L, null, null)
                .pageBreak()
                .row(6L, arrayBlockOf(BIGINT, 7, 8), mapBlockOf(BIGINT, BIGINT, ImmutableMap.of(9, 10, 11, 12)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), true);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 2L, 4L, 5L, 1L)
                .row(1L, 3L, null, null, 2L)
                .row(2L, 99L, null, null, 1L)
                .row(6L, 7L, 9L, 10L, 1L)
                .row(6L, 8L, 11L, 12L, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestNonNumericDoubles()
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayType = metadata.getType(parseTypeSignature("array(double)"));
        Type mapType = metadata.getType(parseTypeSignature("map(bigint,double)"));

        List<Page> input = rowPagesBuilder(BIGINT, arrayType, mapType)
                .row(1L, arrayBlockOf(DOUBLE, NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN),
                        mapBlockOf(BIGINT, DOUBLE, ImmutableMap.of(1, NEGATIVE_INFINITY, 2, POSITIVE_INFINITY, 3, NaN)))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1, 2), ImmutableList.of(arrayType, mapType), false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE, BIGINT, DOUBLE)
                .row(1L, NEGATIVE_INFINITY, 1L, NEGATIVE_INFINITY)
                .row(1L, POSITIVE_INFINITY, 2L, POSITIVE_INFINITY)
                .row(1L, NaN, 3L, NaN)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestWithArrayOfRows()
    {
        MetadataManager metadata = createTestMetadataManager();
        Type arrayOfRowType = metadata.getType(parseTypeSignature("array(row(bigint, double, varchar))"));
        Type elementType = RowType.anonymous(ImmutableList.of(BIGINT, DOUBLE, VARCHAR));

        List<Page> input = rowPagesBuilder(BIGINT, arrayOfRowType)
                .row(1, arrayBlockOf(elementType, ImmutableList.of(2, 4.2, "abc"), ImmutableList.of(3, 6.6, "def")))
                .row(2, arrayBlockOf(elementType, ImmutableList.of(99, 3.14, "pi"), null))
                .row(3, null)
                .pageBreak()
                .row(6, arrayBlockOf(elementType, null, ImmutableList.of(8, 1.111, "tt")))
                .build();

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0, new PlanNodeId("test"), ImmutableList.of(0), ImmutableList.of(BIGINT), ImmutableList.of(1), ImmutableList.of(arrayOfRowType), false);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR)
                .row(1L, 2L, 4.2, "abc")
                .row(1L, 3L, 6.6, "def")
                .row(2L, 99L, 3.14, "pi")
                .row(2L, null, null, null)
                .row(6L, null, null, null)
                .row(6L, 8L, 1.111, "tt")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUnnestSingleArrayUnnester()
    {
        List<Type> replicatedTypes = ImmutableList.of(BIGINT);
        List<Type> unnestTypes = ImmutableList.of(new ArrayType(BIGINT));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(new ArrayType(VARCHAR));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(new ArrayType(new ArrayType(BIGINT)));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    @Test
    public void testUnnestSingleMapUnnester()
    {
        List<Type> replicatedTypes = ImmutableList.of(BIGINT);
        List<Type> unnestTypes = ImmutableList.of(createMapType(BIGINT, BIGINT));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(createMapType(VARCHAR, VARCHAR));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(createMapType(VARCHAR, new ArrayType(BIGINT)));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    @Test
    public void testUnnestSingleArrayOfRowUnnester()
    {
        List<Type> replicatedTypes = ImmutableList.of(BIGINT);
        List<Type> unnestTypes = ImmutableList.of(new ArrayType(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT))));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(new ArrayType(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(
                new ArrayType(withDefaultFieldNames(ImmutableList.of(VARCHAR, new ArrayType(BIGINT), createMapType(VARCHAR, VARCHAR)))));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    @Test
    public void testUnnestTwoArrayUnnesters()
    {
        List<Type> replicatedTypes = ImmutableList.of(BOOLEAN);
        List<Type> unnestTypes = ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(SMALLINT);
        unnestTypes = ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(INTEGER);
        unnestTypes = ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(BIGINT);
        unnestTypes = ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1));
        unnestTypes = ImmutableList.of(
                new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)),
                new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(VARCHAR);
        unnestTypes = ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(BIGINT);
        unnestTypes = ImmutableList.of(new ArrayType(new ArrayType(BIGINT)), new ArrayType(new ArrayType(VARCHAR)));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);

        replicatedTypes = ImmutableList.of(BIGINT);
        unnestTypes = ImmutableList.of(new ArrayType(createMapType(BIGINT, BIGINT)), new ArrayType(createMapType(BIGINT, BIGINT)));
        types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    @Test
    public void testUnnestTwoMapUnnesters()
    {
        List<Type> replicatedTypes = ImmutableList.of(BIGINT);
        List<Type> unnestTypes = ImmutableList.of(createMapType(BIGINT, BIGINT), createMapType(VARCHAR, VARCHAR));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    @Test
    public void testUnnestTwoArrayOfRowUnnesters()
    {
        List<Type> replicatedTypes = ImmutableList.of(BIGINT);
        List<Type> unnestTypes = ImmutableList.of(
                new ArrayType(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER))),
                new ArrayType(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER))));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    @Test
    public void testUnnestMultipleUnnesters()
    {
        List<Type> replicatedTypes = ImmutableList.of(BIGINT);
        List<Type> unnestTypes = ImmutableList.of(
                new ArrayType(BIGINT),
                createMapType(VARCHAR, VARCHAR),
                new ArrayType(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT))),
                new ArrayType(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))));
        List<Type> types = new ArrayList<>(replicatedTypes);
        types.addAll(unnestTypes);

        testUnnest(replicatedTypes, unnestTypes, types);
    }

    protected void testUnnest(List<Type> replicatedTypes, List<Type> unnestTypes, List<Type> types)
    {
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, false, ImmutableList.of());
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, false, ImmutableList.of());
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, false, ImmutableList.of());
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, true, ImmutableList.of());
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, true, ImmutableList.of());
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, true, ImmutableList.of());

        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, false, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, false, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, false, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, false, ImmutableList.of(RUN_LENGTH, DICTIONARY));

        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, false, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, false, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, false, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, false, ImmutableList.of(RUN_LENGTH, DICTIONARY));

        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, false, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, false, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, false, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, false, ImmutableList.of(RUN_LENGTH, DICTIONARY));

        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, true, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, true, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, true, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.0f, true, ImmutableList.of(RUN_LENGTH, DICTIONARY));

        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, true, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, true, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, true, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.0f, 0.2f, true, ImmutableList.of(RUN_LENGTH, DICTIONARY));

        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, true, ImmutableList.of(DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, true, ImmutableList.of(RUN_LENGTH));
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, true, ImmutableList.of(DICTIONARY, DICTIONARY));
        testUnnest(replicatedTypes, unnestTypes, types, 0.2f, 0.2f, true, ImmutableList.of(RUN_LENGTH, DICTIONARY));
    }

    protected void testUnnest(
            List<Type> replicatedTypes,
            List<Type> unnestTypes,
            List<Type> types,
            float primitiveNullRate,
            float nestedNullRate,
            boolean useBlockView,
            List<Encoding> wrappings)
    {
        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < PAGE_COUNT; i++) {
            Page inputPage = PageAssertions.createPageWithRandomData(types, POSITION_COUNT, false, false, primitiveNullRate, nestedNullRate, useBlockView, wrappings);
            inputPages.add(inputPage);
        }

        testUnnest(inputPages, replicatedTypes, unnestTypes, false, false);
        testUnnest(inputPages, replicatedTypes, unnestTypes, true, false);
        testUnnest(inputPages, replicatedTypes, unnestTypes, false, true);
        testUnnest(inputPages, replicatedTypes, unnestTypes, true, true);
    }

    private void testUnnest(List<Page> inputPages, List<Type> replicatedTypes, List<Type> unnestTypes, boolean withOrdinality, boolean legacyUnnest)
    {
        List<Integer> replicatedChannels = IntStream.range(0, replicatedTypes.size()).boxed().collect(Collectors.toList());
        List<Integer> unnestChannels = IntStream.range(replicatedTypes.size(), replicatedTypes.size() + unnestTypes.size()).boxed().collect(Collectors.toList());

        OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                0,
                new PlanNodeId("test"),
                replicatedChannels,
                replicatedTypes,
                unnestChannels,
                unnestTypes,
                withOrdinality);
        Operator unnestOperator = ((UnnestOperator.UnnestOperatorFactory) operatorFactory).createOperator(createDriverContext(), legacyUnnest);

        for (Page inputPage : inputPages) {
            int[] maxCardinalities = calculateMaxCardinalities(inputPage, replicatedTypes, unnestTypes);
            List<Type> outputTypes = buildOutputTypes(replicatedTypes, unnestTypes, withOrdinality, legacyUnnest);

            Page expectedPage = buildExpectedPage(inputPage, replicatedTypes, unnestTypes, outputTypes, maxCardinalities, withOrdinality, legacyUnnest);

            unnestOperator.addInput(inputPage);

            List<Page> outputPages = new ArrayList<>();
            while (true) {
                Page outputPage = unnestOperator.getOutput();

                if (outputPage == null) {
                    break;
                }

                assertTrue(outputPage.getPositionCount() <= 1000);

                outputPages.add(outputPage);
            }

            Page mergedOutputPage = mergePages(outputTypes, outputPages);
            assertPageEquals(outputTypes, mergedOutputPage, expectedPage);
        }
    }

    private DriverContext createDriverContext()
    {
        Session testSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        return createTaskContext(EXECUTOR, SCHEDULER, testSession)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
