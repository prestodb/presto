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
package com.facebook.presto.flightshim;

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.plugin.arrow.ArrowColumnHandle;
import com.facebook.plugin.arrow.ArrowPageSource;
import com.facebook.plugin.arrow.ArrowSplit;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

public class TestArrowBatchSource
{
    private static final int MAX_ROWS_PER_BATCH = 1000;
    private BufferAllocator allocator;
    private TestArrowBlockBuilder arrowBlockBuilder;


    @BeforeClass
    public void setUp()
    {
        // Initialize the Arrow allocator
        allocator = new RootAllocator(Integer.MAX_VALUE);
        arrowBlockBuilder = new TestArrowBlockBuilder(FUNCTION_AND_TYPE_MANAGER);
    }

    @AfterClass
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testPrimitiveTypes()
            throws IOException
    {
        try (IntVector intVector = new IntVector("id", allocator);
                VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector))) {

            final int numValues = 10;
            intVector.allocateNew(numValues);

            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
            }
            /*for (int i = 0; i < values.size(); i++) {
                intVector.setSafe(i, i);
                LocalDateTime dateTime = LocalDateTime.parse(values.get(i));
                // First vector value is explicitly set to 0 to ensure no issues with parsing
                //dateVector.setSafe(i, i == 0 ? 0 : (int) dateTime.toLocalDate().toEpochDay());
                //timeVector.setSafe(i, i == 0 ? 0 : (int) TimeUnit.NANOSECONDS.toMillis(dateTime.toLocalTime().toNanoOfDay()));
                //timestampVector.setSafe(i, i == 0 ? 0 : parseTimestampWithoutTimeZone(values.get(i).replace("T", " ")));
                //expectedBuilder.row(i, dateTime.toLocalDate(), dateTime.toLocalTime(), dateTime);
            }*/

            expectedRoot.setRowCount(numValues);

            TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot);

            try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, MAX_ROWS_PER_BATCH)) {
                assertTrue(arrowBatchSource.nextBatch());
                assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
            }
        }
    }

    @Test
    public void testComplexTypes()
            throws IOException
    {
        try (IntVector intVector = new IntVector("id", allocator);
                ListVector listVectorInt = ListVector.empty("array-int", allocator);
                ListVector listVectorVarchar = ListVector.empty("array-varchar", allocator)) {

            // Add the element vectors
            listVectorInt.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()));
            listVectorVarchar.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
            listVectorInt.allocateNew();
            listVectorVarchar.allocateNew();

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, listVectorInt, listVectorVarchar))) {
                final int numValues = 10;
                final String stringData = "abcdefghijklmnopqrstuvwxyz";
                final UnionListWriter writerInt = listVectorInt.getWriter();
                final UnionListWriter writerVarchar = listVectorVarchar.getWriter();
                for (int i = 0; i < numValues; i++) {
                    intVector.setSafe(i, i);

                    writerInt.setPosition(i);
                    writerInt.startList();
                    writerVarchar.startList();
                    for (int j = 0; j < i % 4; j++) {
                        writerInt.integer().writeInt(i * j);
                        String stringValue = stringData.substring(0, i % stringData.length());
                        writerVarchar.writeVarChar(new Text(stringValue));
                    }
                    writerInt.endList();
                    writerVarchar.endList();
                }
                expectedRoot.setRowCount(numValues);

                TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot);

                try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, MAX_ROWS_PER_BATCH)) {
                    assertTrue(arrowBatchSource.nextBatch());
                    assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
                }
            }
        }
    }

    private static class TestArrowBlockBuilder
            extends ArrowBlockBuilder {

        public TestArrowBlockBuilder(TypeManager typeManager)
        {
            super(typeManager);
        }

        public Type getPrestoType(Field field)
        {
            return getPrestoTypeFromArrowField(field);
        }
    }

    private static class TestArrowPageSource
            implements ConnectorPageSource {
        private final int rowCount;
        private final List<Block> blocks;
        private final List<ColumnMetadata> columns;
        private boolean completed;

        private TestArrowPageSource(int rowCount, List<Block> blocks, List<ColumnMetadata> columns)
        {
            this.rowCount = rowCount;
            this.blocks = blocks;
            this.columns = columns;
        }

        public static TestArrowPageSource create(TestArrowBlockBuilder arrowBlockBuilder, VectorSchemaRoot root)
        {
            List<Block> blocks = new ArrayList<>();
            List<ColumnMetadata> columns = new ArrayList<>();
            for (FieldVector vector : root.getFieldVectors()) {
                ColumnMetadata column = ColumnMetadata.builder()
                        .setName(vector.getName())
                        .setType(arrowBlockBuilder.getPrestoType(vector.getField()))
                        .setNullable(vector.getField().isNullable()).build();
                Block block = arrowBlockBuilder.buildBlockFromFieldVector(vector, column.getType(), null);
                blocks.add(block);
                columns.add(column);
            }

            return new TestArrowPageSource(root.getRowCount(), blocks, columns);
        }

        public List<ColumnMetadata> getColumns()
        {
            return columns;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedPositions()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return completed;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public Page getNextPage()
        {
            Page page = new Page(rowCount, blocks.toArray(new Block[0]));
            completed = true;
            return page;
        }

        @Override
        public void close()
        {
        }
    }
}
