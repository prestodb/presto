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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spiller.FileSingleStreamSpillerFactory;
import com.facebook.presto.spiller.GenericSpillerFactory;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.spiller.SpillerStats;
import com.facebook.presto.spiller.TestingSpillContext;
import com.google.common.collect.ImmutableList;
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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, BIGINT, DOUBLE, createUnboundedVarcharType(), DOUBLE);
    private static final BlockEncodingSerde BLOCK_ENCODING_MANAGER = new BlockEncodingManager();
    private static final Path SPILL_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "spills");

    @Benchmark
    public void write(BenchmarkData data)
            throws ExecutionException, InterruptedException
    {
        try (Spiller spiller = data.createSpiller()) {
            spiller.spill(data.getPages().iterator()).get();
        }
    }

    @Benchmark
    public void read(BenchmarkData data)
    {
        List<Iterator<Page>> spills = data.getReadSpiller().getSpills();
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
        private final SpillerStats spillerStats = new SpillerStats();

        @Param("10000")
        private int rowsPerPage = 10000;

        @Param("10")
        private int pagesCount = 10;

        @Param("false")
        private boolean compressionEnabled;

        @Param("false")
        private boolean encryptionEnabled;

        private List<Page> pages;
        private Spiller readSpiller;

        private FileSingleStreamSpillerFactory singleStreamSpillerFactory;
        private SpillerFactory spillerFactory;

        @Setup
        public void setup()
                throws ExecutionException, InterruptedException
        {
            singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                    MoreExecutors.newDirectExecutorService(),
                    BLOCK_ENCODING_MANAGER,
                    spillerStats,
                    ImmutableList.of(SPILL_PATH),
                    1.0,
                    compressionEnabled,
                    encryptionEnabled);
            spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
            pages = createInputPages();
            readSpiller = spillerFactory.create(TYPES, new TestingSpillContext(), newSimpleAggregatedMemoryContext());
            readSpiller.spill(pages.iterator()).get();
        }

        @TearDown
        public void tearDown()
        {
            readSpiller.close();
            singleStreamSpillerFactory.destroy();
        }

        private List<Page> createInputPages()
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();

            PageBuilder pageBuilder = new PageBuilder(TYPES);
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
                pageBuilder.reset();
            }

            return pages.build();
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Spiller getReadSpiller()
        {
            return readSpiller;
        }

        public Spiller createSpiller()
        {
            return spillerFactory.create(TYPES, new TestingSpillContext(), newSimpleAggregatedMemoryContext());
        }
    }
}
