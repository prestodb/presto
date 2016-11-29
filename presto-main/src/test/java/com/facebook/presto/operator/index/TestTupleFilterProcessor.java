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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestTupleFilterProcessor
{
    @Test
    public void testFullPageFilter()
            throws Exception
    {
        Page tuplePage = Iterables.getOnlyElement(rowPagesBuilder(BIGINT, VARCHAR, DOUBLE)
                .row(1L, "a", 0.1)
                .build());

        List<Type> outputTypes = ImmutableList.of(VARCHAR, BIGINT, BOOLEAN, DOUBLE, DOUBLE);
        TupleFilterProcessor tupleFilterProcessor = new TupleFilterProcessor(tuplePage, outputTypes, new int[] { 1, 0, 3 });

        Page inputPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("b", 1L, true, 0.1, 2.0)
                .row("a", 1L, false, 0.1, 2.0)
                .row("a", 0L, false, 0.2, 0.2)
                .build());

        PageBuilder pageBuilder = new PageBuilder(outputTypes);
        int end = tupleFilterProcessor.process(SESSION, inputPage, 0, inputPage.getPositionCount(), pageBuilder);
        Page actualPage = pageBuilder.build();

        Page expectedPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("a", 1L, false, 0.1, 2.0)
                .build());

        assertEquals(end, inputPage.getPositionCount());
        assertPageEquals(outputTypes, actualPage, expectedPage);
    }

    @Test
    public void testPartialPageFilter()
            throws Exception
    {
        Page tuplePage = Iterables.getOnlyElement(rowPagesBuilder(BIGINT, VARCHAR, DOUBLE)
                .row(1L, "a", 0.1)
                .build());

        List<Type> outputTypes = ImmutableList.of(VARCHAR, BIGINT, BOOLEAN, DOUBLE, DOUBLE);
        TupleFilterProcessor tupleFilterProcessor = new TupleFilterProcessor(tuplePage, outputTypes, new int[] { 1, 0, 3 });

        Page inputPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("b", 1L, true, 0.1, 2.0)
                .row("a", 1L, false, 0.1, 2.0)
                .row("a", 0L, false, 0.2, 0.2)
                .row("a", 1L, false, 0.1, 3.0)
                .build());

        PageBuilder pageBuilder = new PageBuilder(outputTypes);
        int end = tupleFilterProcessor.process(SESSION, inputPage, 1, 4, pageBuilder);
        Page actualPage = pageBuilder.build();

        Page expectedPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, false, 0.1, 2.0)
                .build());

        assertEquals(end, 4);
        assertPageEquals(outputTypes, actualPage, expectedPage);
    }
}
