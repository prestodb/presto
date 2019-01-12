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
package io.prestosql.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingConnectorSession.SESSION;

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
        PageProcessor tupleFilterProcessor = filterFactory.createPageProcessor(tuplePage, OptionalInt.of(MAX_BATCH_SIZE)).get();
        Page actualPage = getOnlyElement(
                tupleFilterProcessor.process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage))
                .orElseThrow(() -> new AssertionError("page is not present"));

        Page expectedPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("a", 1L, false, 0.1, 2.0)
                .build());

        assertPageEquals(outputTypes, actualPage, expectedPage);
    }
}
