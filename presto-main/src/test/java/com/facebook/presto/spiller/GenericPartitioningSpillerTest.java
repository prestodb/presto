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
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class GenericPartitioningSpillerTest
{
    private static final int PARTITIONS_COUNT = 4;
    private static final int FIRST_PARTITION_START = -10;
    private static final int SECOND_PARTITION_START = 0;
    private static final int THIRD_PARTITION_START = 10;
    private static final int FOURTH_PARTITION_START = 20;

    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);
    private static final LocalPartitionGenerator POSITIVE_AND_NEGATIVE_PARTITIONS_HASH_GENERATOR = new FourPartitionsHashGenerator(0);
    private final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(new TypeRegistry(ImmutableSet.of(BIGINT, DOUBLE, VARBINARY)));
    private final BinarySpillerFactory factory = new BinarySpillerFactory(blockEncodingSerde, new FeaturesConfig());

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (PartitioningSpiller spiller = factory.createPartitioningSpiller(TYPES, POSITIVE_AND_NEGATIVE_PARTITIONS_HASH_GENERATOR, PARTITIONS_COUNT, ImmutableSet.of(0, 3))) {
            testSimpleSpiller(spiller);
        }
    }

    private void testSimpleSpiller(PartitioningSpiller spiller)
            throws ExecutionException, InterruptedException
    {
        RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);
        builder.addSequencePage(10, FIRST_PARTITION_START, -5, 0, 5);

        List<Page> firstSpill = builder.build();

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);
        builder.addSequencePage(10, FOURTH_PARTITION_START, 25, 30, 35);

        List<Page> secondSpill = builder.build();

        PartitioningSpiller.PartitioningSpillResult result = spiller.partitionAndSpill(firstSpill.get(0));
        result.isBlocked().get();
        assertEquals(result.getUnspilledPositions().size(), 0);

        result = spiller.partitionAndSpill(firstSpill.get(1));
        result.isBlocked().get();
        assertEquals(result.getUnspilledPositions().size(), 10);

        result = spiller.partitionAndSpill(secondSpill.get(0));
        result.isBlocked().get();
        assertEquals(result.getUnspilledPositions().size(), 0);

        result = spiller.partitionAndSpill(secondSpill.get(1));
        result.isBlocked().get();
        assertEquals(result.getUnspilledPositions().size(), 10);

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);

        List<Page> secondPartition = builder.build();

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);

        List<Page> thirdPartition = builder.build();

        assertSpilledPages(
                TYPES,
                spiller,
                ImmutableList.of(ImmutableList.of(), secondPartition, thirdPartition, ImmutableList.of()));
    }

    private void assertSpilledPages(
            List<Type> types,
            PartitioningSpiller spiller,
            List<List<Page>> expectedPartitions)
    {
        for (int partition = 0; partition < expectedPartitions.size(); partition++) {
            List<Page> actualSpill = ImmutableList.copyOf(spiller.getSpilledPages(partition));
            List<Page> expectedSpill = expectedPartitions.get(partition);

            assertEquals(actualSpill.size(), expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
    }

    private static class FourPartitionsHashGenerator
            extends LocalPartitionGenerator
    {
        private final int hashChannel;

        public FourPartitionsHashGenerator(int hashChannel)
        {
            super(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {hashChannel}), 2);
            this.hashChannel = hashChannel;
        }

        @Override
        public long getRawHash(int position, Page page)
        {
            return page.getBlock(hashChannel).getLong(position, 0);
        }

        @Override
        public int getPartition(int position, Page page)
        {
            if (getRawHash(position, page) >= FOURTH_PARTITION_START) {
                return 3;
            }
            if (getRawHash(position, page) >= THIRD_PARTITION_START) {
                return 2;
            }
            if (getRawHash(position, page) >= SECOND_PARTITION_START) {
                return 1;
            }
            return 0;
        }
    }
}
