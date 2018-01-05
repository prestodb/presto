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

import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterators.getOnlyElement;

public class TestTupleFilterProcessor
{
    @Test
    public void testFilter()
    {
        Page tuplePage = Iterables.getOnlyElement(rowPagesBuilder(BIGINT, VARCHAR, DOUBLE)
                .row(1L, "a", 0.1)
                .build());

        List<Type> outputTypes = ImmutableList.of(VARCHAR, BIGINT, BOOLEAN, DOUBLE, DOUBLE);

        Page inputPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("b", 1L, true, 0.1, 2.0)
                .row("a", 1L, false, 0.1, 2.0)
                .row("a", 0L, false, 0.2, 0.2)
                .build());

        DynamicTupleFilterFactory filterFactory = new DynamicTupleFilterFactory(
                42,
                new PlanNodeId("42"),
                new int[] {0, 1, 2},
                new int[] {1, 0, 3},
                outputTypes,
                new PageFunctionCompiler(createTestMetadataManager(), 0));
        PageProcessor tupleFilterProcessor = filterFactory.createPageProcessor(tuplePage).get();
        Page actualPage = getOnlyElement(tupleFilterProcessor.process(SESSION, new DriverYieldSignal(), inputPage)).orElseThrow(() -> new AssertionError("page is not present"));

        Page expectedPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("a", 1L, false, 0.1, 2.0)
                .build());

        assertPageEquals(outputTypes, actualPage, expectedPage);
    }
}
