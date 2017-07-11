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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.AggregatedMemoryContext;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.lang.Double.doubleToLongBits;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestFileSingleStreamSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

    private final ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
    private final File spillPath = Files.createTempDir();

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        executor.shutdown();
        deleteRecursively(spillPath);
    }

    @Test
    public void testSpill()
            throws Exception
    {
        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(new TypeRegistry(ImmutableSet.copyOf(TYPES))), false);
        PagesSerde serde = serdeFactory.createPagesSerde();
        SpillerStats spillerStats = new SpillerStats();
        LocalMemoryContext memoryContext = new AggregatedMemoryContext().newLocalMemoryContext();
        FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(serde, executor, spillPath.toPath(), spillerStats, bytes -> { }, memoryContext);

        Page page = buildPage();

        assertEquals(memoryContext.getBytes(), 0);
        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertEquals(1, FileUtils.listFiles(spillPath).size());

        // for spilling memory should be accounted only during spill() method is executing
        assertEquals(memoryContext.getBytes(), 0);

        ImmutableList<Page> spilledPages = ImmutableList.copyOf(spiller.getSpilledPages());

        assertEquals(4, spilledPages.size());
        for (int i = 0; i < 4; ++i) {
            PageAssertions.assertPageEquals(TYPES, page, spilledPages.get(i));
        }

        assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
        spiller.close();
        assertEquals(0, FileUtils.listFiles(spillPath).size());
        assertEquals(memoryContext.getBytes(), 0);
    }

    private Page buildPage()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), 1);

        col1.writeLong(42).closeEntry();
        col2.writeLong(doubleToLongBits(43.0)).closeEntry();
        col3.writeLong(doubleToLongBits(43.0)).writeLong(1).closeEntry();

        return new Page(col1.build(), col2.build(), col3.build());
    }
}
