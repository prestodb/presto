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
package com.facebook.presto.operator.index;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;

public class TestTupleFilterProcessor
{
    @Test
    public void testFilter()
    {
        Page tuplePage = rowPagesBuilder(BIGINT, VARCHAR, DOUBLE)
                .row(1L, "a", 0.1)
                .build()
                .stream()
                .collect(onlyElement());

        List<Type> outputTypes = ImmutableList.of(VARCHAR, BIGINT, BOOLEAN, DOUBLE, DOUBLE);

        Page inputPage = rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("b", 1L, true, 0.1, 2.0)
                .row("a", 1L, false, 0.1, 2.0)
                .row("a", 0L, false, 0.2, 0.2)
                .build()
                .stream()
                .collect(onlyElement());

        DynamicTupleFilterFactory filterFactory = new DynamicTupleFilterFactory(
                42,
                new PlanNodeId("42"),
                new int[] {0, 1, 2},
                new int[] {1, 0, 3},
                outputTypes,
                SESSION.getSqlFunctionProperties(),
                SESSION.getSessionFunctions(),
                new PageFunctionCompiler(createTestMetadataManager(), 0));
        PageProcessor tupleFilterProcessor = filterFactory.createPageProcessor(tuplePage, OptionalInt.of(MAX_BATCH_SIZE)).get();
        Page actualPage = getOnlyElement(
                tupleFilterProcessor.process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage))
                .orElseThrow(() -> new AssertionError("page is not present"));

        Page expectedPage = rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("a", 1L, false, 0.1, 2.0)
                .build()
                .stream()
                .collect(onlyElement());

        assertPageEquals(outputTypes, actualPage, expectedPage);
    }
}
