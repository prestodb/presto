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
package com.facebook.presto.operator.spiller;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.spiller.Spiller;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkBinaryFileSpiller
{
    private static final BlockEncodingSerde BLOCK_ENCODING_MANAGER = new BlockEncodingManager(new TypeRegistry(ImmutableSet.of(BIGINT, DOUBLE, VARCHAR)));
    private static final Path SPILL_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "spills");

    @Benchmark
    public void write(BenchmarkData data)
            throws ExecutionException, InterruptedException
    {
        try (Spiller spiller = new BinaryFileSpiller(BLOCK_ENCODING_MANAGER, data.getExecutor(), SPILL_PATH)) {
            spiller.spill(data.getPages().iterator()).get();
        }
    }

    @Benchmark
    public void read(BenchmarkData data)
    {
        List<Iterator<Page>> spills = data.getSpiller().getSpills();
        for (Iterator<Page> spill : spills) {
            while (spill.hasNext()) {
                Page next = spill.next();
                next.getPositionCount();
            }
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private ListeningExecutorService executor;

        @Param({"10000"})
        private int rowsPerPage = 10000;

        @Param({"10"})
        private int pagesCount = 10;

        private List<Page> pages;
        private Spiller spiller;

        @Setup
        public void setup()
                throws ExecutionException, InterruptedException
        {
            executor = listeningDecorator(newSingleThreadScheduledExecutor());
            pages = createInputPages();
            spiller = new BinaryFileSpiller(BLOCK_ENCODING_MANAGER, executor, SPILL_PATH);
            spiller.spill(pages.iterator()).get();
        }

        @TearDown
        public void tearDown()
        {
            spiller.close();
            MoreExecutors.shutdownAndAwaitTermination(executor, 5, SECONDS);
        }

        private List<Page> createInputPages()
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT, DOUBLE, createUnboundedVarcharType(), DOUBLE));
            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
            for (int j = 0; j < pagesCount; j++) {
                Iterator<LineItem> iterator = lineItemGenerator.iterator();
                for (int i = 0; i < rowsPerPage; i++) {
                    pageBuilder.declarePosition();

                    LineItem lineItem = iterator.next();
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.getOrderKey());
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(1), lineItem.getDiscountPercent());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(2), lineItem.getDiscount());
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(3), lineItem.getReturnFlag());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), lineItem.getExtendedPrice());
                }
                pages.add(pageBuilder.build());
            }

            return pages.build();
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Spiller getSpiller()
        {
            return spiller;
        }

        public ListeningExecutorService getExecutor()
        {
            return executor;
        }
    }
}
