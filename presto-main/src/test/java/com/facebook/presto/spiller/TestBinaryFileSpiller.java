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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.Double.doubleToLongBits;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);

    private BlockEncodingSerde blockEncodingSerde;
    private File spillPath = Files.createTempDir();
    private SpillerStats spillerStats;
    private FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private SpillerFactory factory;
    private PagesSerde pagesSerde;
    private AggregatedMemoryContext memoryContext;

    @BeforeMethod
    public void setUp()
    {
        blockEncodingSerde = new BlockEncodingManager();
        spillerStats = new SpillerStats();
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(spillPath.getAbsolutePath());
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        NodeSpillConfig nodeSpillConfig = new NodeSpillConfig();
        singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(blockEncodingSerde, spillerStats, featuresConfig, nodeSpillConfig);
        factory = new GenericSpillerFactory(singleStreamSpillerFactory);
        PagesSerdeFactory pagesSerdeFactory = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"), nodeSpillConfig.isSpillCompressionEnabled());
        pagesSerde = pagesSerdeFactory.createPagesSerde();
        memoryContext = newSimpleAggregatedMemoryContext();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        singleStreamSpillerFactory.destroy();
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (Spiller spiller = factory.create(TYPES, new TestingSpillContext(), memoryContext)) {
            testSimpleSpiller(spiller);
        }
    }

    @Test
    public void testFileVarbinarySpiller()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(null, 1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        col1.writeLong(42).closeEntry();
        col2.writeLong(doubleToLongBits(43.0)).closeEntry();
        col3.writeLong(doubleToLongBits(43.0)).writeLong(1).closeEntry();

        Page page = new Page(col1.build(), col2.build(), col3.build());

        try (Spiller spiller = factory.create(TYPES, new TestingSpillContext(), memoryContext)) {
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
        long spilledBytesBefore = spillerStats.getTotalSpilledBytes();
        long spilledBytes = 0;

        assertEquals(memoryContext.getBytes(), 0);
        for (List<Page> spill : spills) {
            spilledBytes += spill.stream()
                    .mapToLong(page -> pagesSerde.serialize(page).getSizeInBytes())
                    .sum();
            spiller.spill(spill.iterator()).get();
        }
        assertEquals(spillerStats.getTotalSpilledBytes() - spilledBytesBefore, spilledBytes);
        // At this point, the buffers should still be accounted for in the memory context, because
        // the spiller (FileSingleStreamSpiller) doesn't release its memory reservation until it's closed.
        assertEquals(memoryContext.getBytes(), spills.length * FileSingleStreamSpiller.BUFFER_SIZE);

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
        spiller.close();
        assertEquals(memoryContext.getBytes(), 0);
    }
}
