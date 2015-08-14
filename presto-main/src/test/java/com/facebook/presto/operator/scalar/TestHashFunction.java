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
package com.facebook.presto.operator.scalar;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.HashMap;
import io.airlift.slice.Slice;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlock;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestHashFunction
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    @Test
    public void testLongValues()
    {
        int value1 = getHash(Long.SIZE, 444L, "long");
        int value2 = getHash(Long.SIZE, 44L, "long");
        int value3 = getHash(Long.SIZE, 448484848L, "long");
        int value4 = getHash(Long.SIZE, -4848484L, "long");

        long range;
        long expectedValue;

        range = 34848;
        expectedValue = calculateHashValue(range, value1, value2);
        assertFunction("hash(34848, 444, 44)", expectedValue);

        expectedValue = calculateHashValue(range, value1, value3, value4);
        assertFunction("hash(34848, 444, 448484848, -4848484)", expectedValue);
    }

    @Test
    public void testDoubleValues()
    {
        int value1 = getHash(Double.SIZE, 5858.44, "double");
        int value2 = getHash(Double.SIZE, 577.34, "double");
        int value3 = getHash(Double.SIZE, 4848.44, "double");
        int value4 = getHash(Double.SIZE, new Double(5858), "double");

        long range;
        long expectedValue;
        range  = 95858;
        expectedValue = calculateHashValue(range, value1, value2);
        assertFunction("hash(95858, 5858.44, 577.34)", expectedValue);

        expectedValue = calculateHashValue(range, value2, value3, value4);
        assertFunction("hash(95858, 577.34, 4848.44, 5858)", expectedValue);
    }

    @Test
    public void testSliceValues()
    {
        int value1 = getHashVairable("superman");
        int value2 = getHashVairable("hello");
        int value3 = getHashVairable("qubole");
        int value4 = getHashVairable("hadoop");

        long range = 95847;
        long expectedValue = calculateHashValue(range, value1, value2);
        assertFunction("hash(95847, 'superman', 'hello')", expectedValue);

        expectedValue = calculateHashValue(range, value1, value3, value4);
        assertFunction("hash(95847, 'superman', 'qubole', 'hadoop')", expectedValue);
    }

    @Test
    public void testBooleanValues()
    {
        int value1 = getHash(8, true, "boolean");
        int value2 = getHash(8, false, "boolean");

        long range = 44543;
        long expectedValue;
        expectedValue = calculateHashValue(range, value1);
        assertFunction("hash(44543, true)", expectedValue);

        expectedValue = calculateHashValue(range, value1, value2, value2);
        assertFunction("hash(44543, true, false, false)", expectedValue);
    }

    @Test
    public void testNegativeRange()
    {
        int value1 = getHash(Long.SIZE, 444L, "long");
        int value2 = getHash(Long.SIZE, 44L, "long");
        int value3 = getHash(Double.SIZE, 5858.44, "double");
        int value4 = getHash(Double.SIZE, 577.34, "double");

        long range = -5854;
        long expectedValue = calculateHashValue(range, value1, value2);
        assertFunction("hash(-5854, 444, 44)", expectedValue);

        expectedValue = calculateHashValue(range, value3, value4);
        assertFunction("hash(-5854, 5858.44, 577.34)", expectedValue);
    }

    @Test
    public void testArrays()
    {
        ArrayList<Long> values = new ArrayList<Long>();
        values.add(44L);
        values.add(444L);
        int value = getHashSlice(ArrayType.toStackRepresentation(values));
        long range = 5543;
        long expectedValue = calculateHashValue(range, value);
        assertFunction("hash(5543, ARRAY[44, 444])", expectedValue);
    }

    @Test
    public void testMap()
    {
        HashMap<Long, Long> map = new HashMap<Long, Long>();
        map.put(1L, 2L);
        map.put(3L, 4L);
        int value = getHashSlice(MapType.toStackRepresentation(map));
        long range = 5543;
        long expectedValue = calculateHashValue(range, value);
        assertFunction("hash(5543, map(ARRAY[1,3], ARRAY[2,4]))", expectedValue);
     }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "\\QInvalid argument to hash(): NaN\\E")
    public void testWithNaN()
            throws Exception
    {
        functionAssertions.tryEvaluate("hash(58547, sqrt(-1))");
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    private int getHash(int fixedSize, Object value, String desc)
    {
        FixedWidthBlockBuilder fixedWidthBlockBuilder = new FixedWidthBlockBuilder(fixedSize / 8,  new BlockBuilderStatus());

        if (desc.equals("double")) {
            fixedWidthBlockBuilder.writeDouble((double) value).closeEntry();
        }

        if (desc.equals("long")) {
            fixedWidthBlockBuilder.writeLong((long) value).closeEntry();
        }

        if (desc.equals("boolean")) {
            BOOLEAN.writeBoolean(fixedWidthBlockBuilder, (boolean) value);
        }

        FixedWidthBlock fixedWidthBlock = (FixedWidthBlock) fixedWidthBlockBuilder.build();
        return fixedWidthBlock.hash(0, 0, fixedWidthBlock.getLength(0));
    }

    private int getHashSlice(Slice value)
    {
        VariableWidthBlockBuilder variableWidthBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeSlice(variableWidthBlockBuilder, value);
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) variableWidthBlockBuilder.build();
        return variableWidthBlock.hash(0, 0, variableWidthBlock.getLength(0));
    }
    private int getHashVairable(String value)
    {
        VariableWidthBlockBuilder variableWidthBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());

        VARCHAR.writeString(variableWidthBlockBuilder, value);
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) variableWidthBlockBuilder.build();
        return variableWidthBlock.hash(0, 0, variableWidthBlock.getLength(0));
    }

    private long calculateHashValue(long range, int... values)
    {
        long hashValue = 0;
        for (int i = 0; i < values.length; i++) {
            hashValue += values[i];
            hashValue %= range;
        }

        hashValue += range;
        hashValue %= range;
        return hashValue;
    }
}
