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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
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
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
        try (BitVector bitVector = new BitVector("bitVector", allocator);
                TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", allocator);
                SmallIntVector smallIntVector = new SmallIntVector("smallIntVector", allocator);
                IntVector intVector = new IntVector("intVector", allocator);
                BigIntVector longVector = new BigIntVector("bigIntVector", allocator);
                Float4Vector floatVector = new Float4Vector("floatVector", allocator);
                Float8Vector doubleVector = new Float8Vector("doubleVector", allocator);
                VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(bitVector, tinyIntVector, smallIntVector, intVector, longVector, floatVector, doubleVector))) {
            final int numPages = 5;
            final int numValues = 10;
            intVector.allocateNew(numValues);

            for (int i = 0; i < numValues; i++) {
                bitVector.setSafe(i, i % 2);
                tinyIntVector.setSafe(i, i);
                smallIntVector.setSafe(i, i);
                intVector.setSafe(i, i);
                longVector.setSafe(i, i);
                floatVector.setSafe(i, i * 1.1f);
                doubleVector.setSafe(i, i * 1.1);
            }

            // Add null values
            bitVector.setNull(2);
            tinyIntVector.setNull(3);
            smallIntVector.setNull(4);
            intVector.setNull(5);
            longVector.setNull(6);
            floatVector.setNull(7);
            doubleVector.setNull(8);

            expectedRoot.setRowCount(numValues);

            TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot, 5);

            // Set for 1 batch per page
            int batchCount = 0;
            try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, numValues)) {
                while (arrowBatchSource.nextBatch()) {
                    assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
                    ++batchCount;
                }
            }
            assertEquals(batchCount, numPages);
        }
    }

    @Test
    public void testDateTimeTypes()
            throws IOException
    {
        try (IntVector intVector = new IntVector("id", allocator);
                DateDayVector dateVector = new DateDayVector("date", allocator);
                TimeMilliVector timeVector = new TimeMilliVector("time", allocator);
                TimeStampMilliVector timestampVector = new TimeStampMilliVector("timestamp", allocator);
                VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, dateVector, timeVector, timestampVector))) {
            List<String> values = ImmutableList.of(
                    "1970-01-01T00:00:00",
                    "2024-01-01T01:01:01",
                    "2024-01-02T12:00:00",
                    "2112-12-31T23:58:00",
                    "1968-07-05T08:15:12.345");

            for (int i = 0; i < values.size(); i++) {
                intVector.setSafe(i, i);
                LocalDateTime dateTime = LocalDateTime.parse(values.get(i));
                // First vector value is explicitly set to 0 to ensure no issues with parsing
                dateVector.setSafe(i, i == 0 ? 0 : (int) dateTime.toLocalDate().toEpochDay());
                timeVector.setSafe(i, i == 0 ? 0 : (int) TimeUnit.NANOSECONDS.toMillis(dateTime.toLocalTime().toNanoOfDay()));
                timestampVector.setSafe(i, i == 0 ? 0 : parseTimestampWithoutTimeZone(values.get(i).replace("T", " ")));
            }

            // Add null values at last row
            dateVector.setNull(values.size());
            timeVector.setNull(values.size());
            timestampVector.setNull(values.size());

            expectedRoot.setRowCount(values.size() + 1);

            TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot, 1);

            try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, MAX_ROWS_PER_BATCH)) {
                assertTrue(arrowBatchSource.nextBatch());
                assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
            }
        }
    }

    @Test
    public void testArrayType()
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

            final int numValues = 10;
            final String stringData = "abcdefghijklmnopqrstuvwxyz";
            final UnionListWriter writerInt = listVectorInt.getWriter();
            final UnionListWriter writerVarchar = listVectorVarchar.getWriter();
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                writerInt.setPosition(i);
                // Need to set nulls during write for lists
                if (i == 5) {
                    writerInt.writeNull();
                    writerVarchar.writeNull();
                }
                else {
                    writerInt.startList();
                    writerVarchar.startList();
                    for (int j = 0; j < i + i; j++) {
                        writerInt.integer().writeInt(i * j);
                        String stringValue = stringData.substring(0, i % stringData.length());
                        writerVarchar.writeVarChar(new Text(stringValue));
                    }
                }
                writerInt.endList();
                writerVarchar.endList();
            }

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, listVectorInt, listVectorVarchar))) {
                expectedRoot.setRowCount(numValues);

                TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot, 1);

                try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, MAX_ROWS_PER_BATCH)) {
                    assertTrue(arrowBatchSource.nextBatch());
                    assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
                }
            }
        }
    }

    @Test
    void testMapType()
            throws IOException
    {
        try (IntVector intVector = new IntVector("id", allocator);
                MapVector mapLongVector = MapVector.empty("map-long-long", allocator, false);
                MapVector mapVarcharVector = MapVector.empty("map-long-varchar", allocator, false)) {
            UnionMapWriter mapLongWriter = mapLongVector.getWriter();
            UnionMapWriter mapVarcharWriter = mapVarcharVector.getWriter();
            mapLongWriter.allocate();
            mapVarcharWriter.allocate();

            final int numValues = 10;
            final String stringData = "abcdefghijklmnopqrstuvwxyz";
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                mapLongWriter.setPosition(i);
                mapLongWriter.startMap();

                mapVarcharWriter.setPosition(i);
                mapVarcharWriter.startMap();

                for (int j = 0; j < i; j++) {
                    mapLongWriter.startEntry();
                    mapLongWriter.key().bigInt().writeBigInt(j);
                    mapLongWriter.value().bigInt().writeBigInt(i * j);
                    mapLongWriter.endEntry();
                    mapVarcharWriter.startEntry();
                    mapVarcharWriter.key().bigInt().writeBigInt(j * j);
                    String stringValue = stringData.substring(0, i % stringData.length());
                    mapVarcharWriter.value().varChar().writeVarChar(new Text(stringValue));
                    mapVarcharWriter.endEntry();
                }
                mapLongWriter.endMap();
                mapVarcharWriter.endMap();
            }

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, mapLongVector, mapVarcharVector))) {
                expectedRoot.setRowCount(numValues);

                TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot, 1);

                try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, MAX_ROWS_PER_BATCH)) {
                    assertTrue(arrowBatchSource.nextBatch());
                    assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
                }
            }
        }
    }

    @Test
    void testRowType()
            throws IOException
    {
        try (IntVector intVector = new IntVector("id", allocator);
                StructVector structVector = StructVector.empty("struct", allocator)) {
            final BigIntVector childLongVector
                    = structVector.addOrGet("long", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
            final VarCharVector childVarcharVector
                    = structVector.addOrGet("varchar", FieldType.nullable(ArrowType.Utf8.INSTANCE), VarCharVector.class);
            childLongVector.allocateNew();
            childVarcharVector.allocateNew();

            final int numValues = 10;
            final String stringData = "abcdefghijklmnopqrstuvwxyz";
            for (int i = 0; i < numValues; i++) {
                intVector.setSafe(i, i);
                childLongVector.setSafe(i, i * i);
                String stringValue = stringData.substring(0, i % stringData.length());
                childVarcharVector.setSafe(i, new Text(stringValue));
                structVector.setIndexDefined(i);
            }

            try (VectorSchemaRoot expectedRoot = new VectorSchemaRoot(Arrays.asList(intVector, structVector))) {
                expectedRoot.setRowCount(numValues);

                TestArrowPageSource pageSource = TestArrowPageSource.create(arrowBlockBuilder, expectedRoot, 1);

                try (ArrowBatchSource arrowBatchSource = new ArrowBatchSource(allocator, pageSource.getColumns(), pageSource, MAX_ROWS_PER_BATCH)) {
                    assertTrue(arrowBatchSource.nextBatch());
                    assertTrue(expectedRoot.equals(arrowBatchSource.getVectorSchemaRoot()));
                }
            }
        }
    }

    private static class TestArrowBlockBuilder
            extends ArrowBlockBuilder
    {
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
            implements ConnectorPageSource
    {
        private final int rowCount;
        private final List<Block> blocks;
        private final List<ColumnMetadata> columns;
        private int pagesRemaining;

        private TestArrowPageSource(int rowCount, List<Block> blocks, List<ColumnMetadata> columns, int numPages)
        {
            this.rowCount = rowCount;
            this.blocks = blocks;
            this.columns = columns;
            this.pagesRemaining = numPages;
        }

        public static TestArrowPageSource create(TestArrowBlockBuilder arrowBlockBuilder, VectorSchemaRoot root, int numPages)
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

            return new TestArrowPageSource(root.getRowCount(), blocks, columns, numPages);
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
            return pagesRemaining == 0;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public Page getNextPage()
        {
            if (pagesRemaining == 0) {
                return null;
            }
            --pagesRemaining;
            return new Page(rowCount, blocks.toArray(new Block[0]));
        }

        @Override
        public void close()
        {
        }
    }
}
