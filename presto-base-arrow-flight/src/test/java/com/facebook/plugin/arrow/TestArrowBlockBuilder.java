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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestArrowBlockBuilder
{
    private static final Logger logger = Logger.get(TestArrowBlockBuilder.class);
    private static final int DICTIONARY_LENGTH = 10;
    private static final int VECTOR_LENGTH = 50;
    private BufferAllocator allocator;
    private ArrowBlockBuilder arrowBlockBuilder;

    @BeforeClass
    public void setUp()
    {
        // Initialize the Arrow allocator
        allocator = new RootAllocator(Integer.MAX_VALUE);
        logger.debug("Allocator initialized: %s", allocator.getName());
        arrowBlockBuilder = new ArrowBlockBuilder(FUNCTION_AND_TYPE_MANAGER);
    }

    @AfterClass
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testBuildBlockFromBitVector()
    {
        // Create a BitVector and populate it with values
        try (BitVector bitVector = new BitVector("bitVector", allocator)) {
            bitVector.allocateNew(3);  // Allocating space for 3 elements

            bitVector.set(0, 1);  // Set value to 1 (true)
            bitVector.set(1, 0);  // Set value to 0 (false)
            bitVector.setNull(2);  // Set null value

            bitVector.setValueCount(3);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(bitVector, BooleanType.BOOLEAN, null);

            // Now verify the result block
            assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
            assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
        }
    }

    @Test
    public void testBuildBlockFromTinyIntVector()
    {
        // Create a TinyIntVector and populate it with values
        try (TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", allocator)) {
            tinyIntVector.allocateNew(3);  // Allocating space for 3 elements
            tinyIntVector.set(0, 10);
            tinyIntVector.set(1, 20);
            tinyIntVector.setNull(2);  // Set null value

            tinyIntVector.setValueCount(3);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(tinyIntVector, TinyintType.TINYINT, null);

            // Now verify the result block
            assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
            assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
        }
    }

    @Test
    public void testBuildBlockFromSmallIntVector()
    {
        // Create a SmallIntVector and populate it with values
        try (SmallIntVector smallIntVector = new SmallIntVector("smallIntVector", allocator)) {
            smallIntVector.allocateNew(3);  // Allocating space for 3 elements
            smallIntVector.set(0, 10);
            smallIntVector.set(1, 20);
            smallIntVector.setNull(2);  // Set null value

            smallIntVector.setValueCount(3);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(smallIntVector, SmallintType.SMALLINT, null);

            // Now verify the result block
            assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
            assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
        }
    }

    @Test
    public void testBuildBlockFromIntVector()
    {
        // Create an IntVector and populate it with values
        try (IntVector intVector = new IntVector("intVector", allocator)) {
            intVector.allocateNew(3);  // Allocating space for 3 elements
            intVector.set(0, 10);
            intVector.set(1, 20);
            intVector.set(2, 30);

            intVector.setValueCount(3);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(intVector, IntegerType.INTEGER, null);

            // Now verify the result block
            assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
            assertEquals(10, resultBlock.getInt(0));  // The 1st element should be 10
            assertEquals(20, resultBlock.getInt(1));  // The 2nd element should be 20
            assertEquals(30, resultBlock.getInt(2));  // The 3rd element should be 30
        }
    }

    @Test
    public void testBuildBlockFromBigIntVector()
            throws InstantiationException, IllegalAccessException
    {
        // Create a BigIntVector and populate it with values
        try (BigIntVector bigIntVector = new BigIntVector("bigIntVector", allocator)) {
            bigIntVector.allocateNew(3);  // Allocating space for 3 elements

            bigIntVector.set(0, 10L);
            bigIntVector.set(1, 20L);
            bigIntVector.set(2, 30L);

            bigIntVector.setValueCount(3);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(bigIntVector, BigintType.BIGINT, null);

            // Now verify the result block
            assertEquals(10L, resultBlock.getInt(0));  // The 1st element should be 10L
            assertEquals(20L, resultBlock.getInt(1));  // The 2nd element should be 20L
            assertEquals(30L, resultBlock.getInt(2));  // The 3rd element should be 30L
        }
    }

    @Test
    public void testBuildBlockFromDecimalVector()
    {
        // Create a DecimalVector and populate it with values
        try (DecimalVector decimalVector = new DecimalVector("decimalVector", allocator, 10, 2)) {  // Precision = 10, Scale = 2
            decimalVector.allocateNew(2);  // Allocating space for 2 elements
            decimalVector.set(0, new BigDecimal("123.45"));

            decimalVector.setValueCount(2);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(decimalVector, DecimalType.createDecimalType(10, 2), null);

            // Now verify the result block
            assertEquals(2, resultBlock.getPositionCount());  // Should have 2 positions
            assertTrue(resultBlock.isNull(1));  // The 2nd element should be null
        }
    }

    @Test
    public void testBuildBlockFromTimeStampMicroVector()
    {
        // Create a TimeStampMicroVector and populate it with values
        try (TimeStampMicroVector timestampMicroVector = new TimeStampMicroVector("timestampMicroVector", allocator)) {
            timestampMicroVector.allocateNew(3);  // Allocating space for 3 elements
            timestampMicroVector.set(0, 1000000L);  // 1 second in microseconds
            timestampMicroVector.set(1, 2000000L);  // 2 seconds in microseconds
            timestampMicroVector.setNull(2);  // Set null value

            timestampMicroVector.setValueCount(3);

            // Build the block from the vector
            Block resultBlock = arrowBlockBuilder.buildBlockFromFieldVector(timestampMicroVector, TimestampType.TIMESTAMP, null);

            // Now verify the result block
            assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
            assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
            assertEquals(1000L, resultBlock.getLong(0));  // The 1st element should be 1000ms (1 second)
            assertEquals(2000L, resultBlock.getLong(1));  // The 2nd element should be 2000ms (2 seconds)
        }
    }

    @Test
    public void testBuildBlockFromListVector()
    {
        // Create a root allocator for Arrow vectors
        try (BufferAllocator allocator = new RootAllocator();
                ListVector listVector = ListVector.empty("listVector", allocator)) {
            // Allocate the vector and get the writer
            listVector.allocateNew();
            UnionListWriter listWriter = listVector.getWriter();

            int[] data = new int[] {1, 2, 3, 10, 20, 30, 100, 200, 300, 1000, 2000, 3000};
            int tmpIndex = 0;

            for (int i = 0; i < 4; i++) { // 4 lists to be added
                listWriter.startList();
                for (int j = 0; j < 3; j++) { // Each list has 3 integers
                    listWriter.writeInt(data[tmpIndex]);
                    tmpIndex++;
                }
                listWriter.endList();
            }

            // Set the number of lists
            listVector.setValueCount(4);

            // Create Presto ArrayType for Integer
            ArrayType arrayType = new ArrayType(IntegerType.INTEGER);

            // Call the method to test
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(listVector, arrayType, null);
            assertTrue(block instanceof ArrayBlock);

            // Validate the result
            assertEquals(block.getPositionCount(), 4); // 4 lists in the block
            for (int j = 0; j < block.getPositionCount(); j++) {
                Block subBlock = block.getBlock(j);
                assertEquals(subBlock.getPositionCount(), 3); // each list should have 3 elements
            }
        }
    }

    @Test
    public void testProcessDictionaryVector()
    {
        try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
                VarCharVector rawVector = new VarCharVector("raw", allocator)) {
            dictionaryVector.allocateNew(DICTIONARY_LENGTH);
            for (int i = 0; i < DICTIONARY_LENGTH; i++) {
                dictionaryVector.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            }
            dictionaryVector.setValueCount(DICTIONARY_LENGTH);

            rawVector.allocateNew(VECTOR_LENGTH);
            for (int i = 0; i < VECTOR_LENGTH; i++) {
                int value = i % DICTIONARY_LENGTH;
                rawVector.setSafe(i, String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            }
            rawVector.setValueCount(VECTOR_LENGTH);

            // Encode the vector with a dictionary
            Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, new ArrowType.Int(16, true)));
            try (ValueVector encodedVector = DictionaryEncoder.encode(rawVector, dictionary);
                    DictionaryProvider.MapDictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(dictionary)) {
                // Process the dictionary vector
                assertTrue(encodedVector instanceof FieldVector);
                Block result = arrowBlockBuilder.buildBlockFromFieldVector((FieldVector) encodedVector, VarcharType.VARCHAR, dictionaryProvider);

                // Verify the result
                assertNotNull(result, "The BlockBuilder should not be null.");
                assertEquals(result.getPositionCount(), 50);
            }
        }
    }

    @Test
    public void testBuildBlockFromDictionaryVector()
    {
        // Initialize a dictionary vector
        // Example: dictionary contains 3 string values
        VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        dictionaryVector.allocateNew(3); // allocating 3 elements in dictionary

        // Fill dictionaryVector with some values
        dictionaryVector.set(0, "apple".getBytes());
        dictionaryVector.set(1, "banana".getBytes());
        dictionaryVector.set(2, "cherry".getBytes());
        dictionaryVector.setValueCount(3);

        Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)));

        FieldType indexFieldType = new FieldType(false, dictionary.getEncoding().getIndexType(), dictionary.getEncoding());
        Field indexField = new Field("indices", indexFieldType, null);
        try (DictionaryProvider.MapDictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(dictionary);
                IntVector indicesVector = (IntVector) indexField.createVector(allocator)) {
            indicesVector.allocateNew(4); // allocating space for  values

            // Set up index values (this would reference the dictionary)
            indicesVector.set(0, 0);  // First index points to "apple"
            indicesVector.set(1, 1);  // Second index points to "banana"
            indicesVector.set(2, 2);
            indicesVector.set(3, 2); // Third index points to "cherry"
            indicesVector.setValueCount(4);

            // Call the method under test
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(indicesVector, VarcharType.VARCHAR, dictionaryProvider);

            // Assertions to check the dictionary block's behavior
            assertNotNull(block);
            assertTrue(block instanceof DictionaryBlock);
            DictionaryBlock dictionaryBlock = (DictionaryBlock) block;

            // Verify the dictionary block contains the right dictionary
            for (int i = 0; i < dictionaryBlock.getPositionCount(); i++) {
                // Get the slice (string value) at the given position
                Slice slice = dictionaryBlock.getSlice(i, 0, dictionaryBlock.getSliceLength(i));

                // Assert based on the expected values
                if (i == 0) {
                    assertEquals(slice.toStringUtf8(), "apple");
                }
                else if (i == 1) {
                    assertEquals(slice.toStringUtf8(), "banana");
                }
                else if (i == 2) {
                    assertEquals(slice.toStringUtf8(), "cherry");
                }
                else if (i == 3) {
                    assertEquals(slice.toStringUtf8(), "cherry");
                }
            }
        }
    }

    @Test
    public void testBuildBlockFromDictionaryVectorSmallInt()
    {
        // Initialize a dictionary vector
        // Example: dictionary contains 3 string values
        VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        dictionaryVector.allocateNew(3); // allocating 3 elements in dictionary

        // Fill dictionaryVector with some values
        dictionaryVector.set(0, "apple".getBytes());
        dictionaryVector.set(1, "banana".getBytes());
        dictionaryVector.set(2, "cherry".getBytes());
        dictionaryVector.setValueCount(3);

        Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, new ArrowType.Int(16, true)));

        FieldType indexFieldType = new FieldType(false, dictionary.getEncoding().getIndexType(), dictionary.getEncoding());
        Field indexField = new Field("indices", indexFieldType, null);
        try (DictionaryProvider.MapDictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(dictionary);
                SmallIntVector indicesVector = (SmallIntVector) indexField.createVector(allocator)) {
            indicesVector.allocateNew(3); // allocating space for 3 values
            indicesVector.set(0, (short) 0);
            indicesVector.set(1, (short) 1);
            indicesVector.set(2, (short) 2);
            indicesVector.setValueCount(3);

            // Call the method under test
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(indicesVector, VarcharType.VARCHAR, dictionaryProvider);

            // Assertions to check the dictionary block's behavior
            assertNotNull(block);
            assertTrue(block instanceof DictionaryBlock);
            DictionaryBlock dictionaryBlock = (DictionaryBlock) block;

            // Verify the dictionary block contains the right dictionary
            for (int i = 0; i < dictionaryBlock.getPositionCount(); i++) {
                // Get the slice (string value) at the given position
                Slice slice = dictionaryBlock.getSlice(i, 0, dictionaryBlock.getSliceLength(i));

                // Assert based on the expected values
                if (i == 0) {
                    assertEquals(slice.toStringUtf8(), "apple");
                }
                else if (i == 1) {
                    assertEquals(slice.toStringUtf8(), "banana");
                }
                else if (i == 2) {
                    assertEquals(slice.toStringUtf8(), "cherry");
                }
            }
        }
    }

    @Test
    public void testBuildBlockFromDictionaryVectorTinyInt()
    {
        // Initialize a dictionary vector
        // Example: dictionary contains 3 string values
        VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        dictionaryVector.allocateNew(3); // allocating 3 elements in dictionary

        // Fill dictionaryVector with some values
        dictionaryVector.set(0, "apple".getBytes());
        dictionaryVector.set(1, "banana".getBytes());
        dictionaryVector.set(2, "cherry".getBytes());
        dictionaryVector.setValueCount(3);

        Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, new ArrowType.Int(8, true)));

        FieldType indexFieldType = new FieldType(false, dictionary.getEncoding().getIndexType(), dictionary.getEncoding());
        Field indexField = new Field("indices", indexFieldType, null);
        try (DictionaryProvider.MapDictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(dictionary);
                TinyIntVector indicesVector = (TinyIntVector) indexField.createVector(allocator)) {
            indicesVector.allocateNew(3); // allocating space for 3 values
            indicesVector.set(0, (byte) 0);
            indicesVector.set(1, (byte) 1);
            indicesVector.set(2, (byte) 2);
            indicesVector.setValueCount(3);

            // Call the method under test
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(indicesVector, VarcharType.VARCHAR, dictionaryProvider);

            // Assertions to check the dictionary block's behavior
            assertNotNull(block);
            assertTrue(block instanceof DictionaryBlock);
            DictionaryBlock dictionaryBlock = (DictionaryBlock) block;

            // Verify the dictionary block contains the right dictionary
            for (int i = 0; i < dictionaryBlock.getPositionCount(); i++) {
                // Get the slice (string value) at the given position
                Slice slice = dictionaryBlock.getSlice(i, 0, dictionaryBlock.getSliceLength(i));

                // Assert based on the expected values
                if (i == 0) {
                    assertEquals(slice.toStringUtf8(), "apple");
                }
                else if (i == 1) {
                    assertEquals(slice.toStringUtf8(), "banana");
                }
                else if (i == 2) {
                    assertEquals(slice.toStringUtf8(), "cherry");
                }
            }
        }
    }

    @Test
    public void testAssignVarcharType()
    {
        try (VarCharVector vector = new VarCharVector("varCharVector", allocator)) {
            vector.allocateNew(3);

            String value = "test_string";
            vector.set(0, new Text(value));
            vector.setValueCount(1);

            Type varcharType = VarcharType.createUnboundedVarcharType();
            BlockBuilder builder = varcharType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromVarCharVector(vector, varcharType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            Slice result = varcharType.getSlice(block, 0);
            assertEquals(result.toStringUtf8(), value);
        }
    }

    @Test
    public void testAssignSmallintType()
    {
        try (SmallIntVector vector = new SmallIntVector("smallIntVector", allocator)) {
            vector.allocateNew(3);

            short value = 42;
            vector.set(0, value);
            vector.setValueCount(1);

            Type smallintType = SmallintType.SMALLINT;
            BlockBuilder builder = smallintType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromSmallIntVector(vector, smallintType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = smallintType.getLong(block, 0);
            assertEquals(result, value);
        }
    }

    @Test
    public void testAssignTinyintType()
    {
        try (TinyIntVector vector = new TinyIntVector("tinyIntVector", allocator)) {
            vector.allocateNew(3);

            byte value = 7;
            vector.set(0, value);
            vector.setValueCount(1);

            Type tinyintType = TinyintType.TINYINT;
            BlockBuilder builder = tinyintType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromTinyIntVector(vector, tinyintType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = tinyintType.getLong(block, 0);
            assertEquals(result, value);
        }
    }

    @Test
    public void testAssignBigintType()
    {
        try (BigIntVector vector = new BigIntVector("bigIntVector", allocator)) {
            vector.allocateNew(3);

            long value = 123456789L;
            vector.set(0, value);
            vector.setValueCount(1);

            Type bigintType = BigintType.BIGINT;
            BlockBuilder builder = bigintType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromBigIntVector(vector, bigintType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = bigintType.getLong(block, 0);
            assertEquals(result, value);
        }
    }

    @Test
    public void testAssignIntegerType()
    {
        try (IntVector vector = new IntVector("IntVector", allocator)) {
            vector.allocateNew(3);

            int value = 42;
            vector.set(0, value);
            vector.setValueCount(1);

            Type integerType = IntegerType.INTEGER;
            BlockBuilder builder = integerType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromIntVector(vector, integerType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = integerType.getLong(block, 0);
            assertEquals(result, value);
        }
    }

    @Test
    public void testAssignDoubleType()
    {
        try (Float8Vector vector = new Float8Vector("Float8Vector", allocator)) {
            vector.allocateNew(3);

            double value = 42.42;
            vector.set(0, value);
            vector.setValueCount(1);

            Type doubleType = DoubleType.DOUBLE;
            BlockBuilder builder = doubleType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromFloat8Vector(vector, doubleType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            double result = doubleType.getDouble(block, 0);
            assertEquals(result, value, 0.001);
        }
    }

    @Test
    public void testAssignBooleanType()
    {
        try (BitVector vector = new BitVector("BitVector", allocator)) {
            vector.allocateNew(3);

            boolean value = true;
            vector.set(0, 1);
            vector.setValueCount(1);

            Type booleanType = BooleanType.BOOLEAN;
            BlockBuilder builder = booleanType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromBitVector(vector, booleanType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            boolean result = booleanType.getBoolean(block, 0);
            assertEquals(result, value);
        }
    }

    @Test
    public void testAssignArrayType()
    {
        try (ListVector vector = ListVector.empty("ListVector", allocator)) {
            UnionListWriter writer = vector.getWriter();
            writer.allocate();

            writer.setPosition(0); // optional
            writer.startList();
            writer.integer().writeInt(1);
            writer.integer().writeInt(2);
            writer.integer().writeInt(3);
            writer.endList();

            writer.setValueCount(1);

            Type elementType = IntegerType.INTEGER;
            ArrayType arrayType = new ArrayType(elementType);
            BlockBuilder builder = arrayType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromListVector(vector, arrayType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            List<Integer> values = Arrays.asList(1, 2, 3);
            Block arrayBlock = arrayType.getObject(block, 0);
            assertEquals(arrayBlock.getPositionCount(), values.size());
            for (int i = 0; i < values.size(); i++) {
                assertEquals(elementType.getLong(arrayBlock, i), values.get(i).longValue());
            }
        }
    }

    @Ignore("RowType not implemented")
    @Test
    public void testAssignRowType()
    {
        RowType.Field field1 = new RowType.Field(Optional.of("field1"), IntegerType.INTEGER);
        RowType.Field field2 = new RowType.Field(Optional.of("field2"), VarcharType.createUnboundedVarcharType());
        RowType rowType = RowType.from(Arrays.asList(field1, field2));
        BlockBuilder builder = rowType.createBlockBuilder(null, 1);

        List<Object> rowValues = Arrays.asList(42, "test");
        // TODO: arrowBlockBuilder.(rowType, builder, rowValues);

        Block block = builder.build();
        Block rowBlock = rowType.getObject(block, 0);
        assertEquals(IntegerType.INTEGER.getLong(rowBlock, 0), 42);
        assertEquals(VarcharType.createUnboundedVarcharType().getSlice(rowBlock, 1).toStringUtf8(), "test");
    }

    @Test
    public void testAssignDateType()
    {
        try (DateDayVector vector = new DateDayVector("DateDayVector", allocator)) {
            vector.allocateNew(3);

            LocalDate value = LocalDate.of(2020, 1, 1);
            vector.set(0, (int) value.toEpochDay());
            vector.setValueCount(1);

            Type dateType = DateType.DATE;
            BlockBuilder builder = dateType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromDateDayVector(vector, dateType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = dateType.getLong(block, 0);
            assertEquals(result, value.toEpochDay());
        }
    }

    @Test
    public void testAssignTimestampType()
    {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("TimeStampMilliVector", allocator)) {
            vector.allocateNew(3);

            long value = 1609459200000L; // Jan 1, 2021, 00:00:00 UTC
            vector.set(0, value);
            vector.setValueCount(1);

            Type timestampType = TimestampType.TIMESTAMP;
            BlockBuilder builder = timestampType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromTimeStampMilliVector(vector, timestampType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = timestampType.getLong(block, 0);
            assertEquals(result, value);
        }
    }

    @Test
    public void testAssignTimestampTypeWithSqlTimestamp()
    {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("TimeStampMilliVector", allocator)) {
            vector.allocateNew(3);

            java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2021-01-01 00:00:00");
            long expectedMillis = timestamp.getTime();
            vector.set(0, expectedMillis);
            vector.setValueCount(1);

            Type timestampType = TimestampType.TIMESTAMP;
            BlockBuilder builder = timestampType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromTimeStampMilliVector(vector, timestampType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long result = timestampType.getLong(block, 0);
            assertEquals(result, expectedMillis);
        }
    }

    @Test
    public void testAssignShortDecimal()
    {
        try (DecimalVector vector = new DecimalVector("DecimalVector", allocator, 10, 2)) {
            vector.allocateNew(3);

            BigDecimal decimalValue = new BigDecimal("12345.67");
            vector.set(0, decimalValue);
            vector.setValueCount(1);

            DecimalType shortDecimalType = DecimalType.createDecimalType(10, 2); // Precision: 10, Scale: 2
            BlockBuilder builder = shortDecimalType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromDecimalVector(vector, shortDecimalType, builder, 0, vector.getValueCount());

            Block block = builder.build();
            long unscaledValue = shortDecimalType.getLong(block, 0); // Unscaled value: 1234567
            BigDecimal result = BigDecimal.valueOf(unscaledValue).movePointLeft(shortDecimalType.getScale());
            assertEquals(result, decimalValue);
        }
    }

    @Test
    public void testAssignLongDecimal()
    {
        try (DecimalVector vector = new DecimalVector("DecimalVector", allocator, 38, 10)) {
            vector.allocateNew(3);

            BigDecimal decimalValue = new BigDecimal("1234567890.1234567890");
            vector.set(0, decimalValue);
            vector.setValueCount(1);

            // Create a DecimalType with precision 38 and scale 10
            DecimalType longDecimalType = DecimalType.createDecimalType(38, 10);
            BlockBuilder builder = longDecimalType.createBlockBuilder(null, 1);

            arrowBlockBuilder.assignBlockFromDecimalVector(vector, longDecimalType, builder, 0, vector.getValueCount());

            // Build the block after inserting the decimal value
            Block block = builder.build();
            Slice unscaledSlice = longDecimalType.getSlice(block, 0);
            BigInteger unscaledValue = Decimals.decodeUnscaledValue(unscaledSlice);
            BigDecimal result = new BigDecimal(unscaledValue).movePointLeft(longDecimalType.getScale());
            // Assert the decoded result is equal to the original decimal value
            assertEquals(result, decimalValue);
        }
    }

    @Test
    public void testVarcharVector()
    {
        try (VarCharVector vector = new VarCharVector("VarCharVector", allocator)) {
            vector.allocateNew(3);

            vector.set(0, new Text("apple").getBytes());
            vector.set(1, new Text("fig").getBytes());
            vector.setValueCount(2);

            Block resultblock = arrowBlockBuilder.buildBlockFromFieldVector(vector, VarcharType.VARCHAR, null);

            assertEquals(2, resultblock.getPositionCount());

            // Extract values from the Block and compare with the values in the vector
            for (int i = 0; i < vector.getValueCount(); i++) {
                // Retrieve the value as a Slice for the ith position in the Block
                Slice slice = resultblock.getSlice(i, 0, resultblock.getSliceLength(i));
                // Assert based on the expected values
                assertEquals(slice.toStringUtf8(), new String(vector.get(i)));
            }
        }
    }
}
