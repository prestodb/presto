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
package com.facebook.presto.spiller;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.doubleToLongBits;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);
    private final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(new TypeRegistry(ImmutableSet.of(BIGINT, DOUBLE, VARBINARY)));
    private final BinarySpillerFactory factory = new BinarySpillerFactory(blockEncodingSerde, new FeaturesConfig());

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (Spiller spiller = factory.create(TYPES)) {
            testSimpleSpiller(spiller);
        }
    }

    @Test
    public void testFileVarbinarySpiller()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

        BlockBuilder col1 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder col3 = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1);

        col1.writeLong(42).closeEntry();
        col2.writeLong(doubleToLongBits(43.0)).closeEntry();
        col3.writeLong(doubleToLongBits(43.0)).writeLong(1).closeEntry();

        Page page = new Page(col1.build(), col2.build(), col3.build());

        try (Spiller spiller = factory.create(TYPES)) {
            testSpiller(types, spiller, ImmutableList.of(page));
        }
    }

    private void testSimpleSpiller(Spiller spiller)
            throws ExecutionException, InterruptedException
    {
        RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 0, 5, 10, 15);
        builder.pageBreak();
        builder.addSequencePage(10, 0, -5, -10, -15);
        List<Page> firstSpill = builder.build();

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 10, 15, 20, 25);
        builder.pageBreak();
        builder.addSequencePage(10, -10, -15, -20, -25);
        List<Page> secondSpill = builder.build();

        testSpiller(TYPES, spiller, firstSpill, secondSpill);
    }

    private void testSpiller(List<Type> types, Spiller spiller, List<Page>... spills)
            throws ExecutionException, InterruptedException
    {
        long spilledBytesBefore = factory.getSpilledBytes();
        long spilledBytes = 0;
        for (List<Page> spill : spills) {
            spilledBytes += spill.stream().mapToLong(Page::getSizeInBytes).sum();
            spiller.spill(spill.iterator()).get();
        }
        assertEquals(factory.getSpilledBytes() - spilledBytesBefore, spilledBytes);

        List<Iterator<Page>> actualSpills = spiller.getSpills();
        assertEquals(actualSpills.size(), spills.length);

        for (int i = 0; i < actualSpills.size(); i++) {
            List<Page> actualSpill = ImmutableList.copyOf(actualSpills.get(i));
            List<Page> expectedSpill = spills[i];

            assertEquals(actualSpill.size(), expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
    }
}
