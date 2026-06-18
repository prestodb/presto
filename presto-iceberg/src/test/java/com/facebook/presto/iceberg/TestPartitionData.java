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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestPartitionData
{
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    @Test
    public void testGetValueWithNull()
    {
        JsonNode nullNode = JSON.nullNode();
        assertNull(PartitionData.getValue(nullNode, Types.IntegerType.get()));
        assertNull(PartitionData.getValue(nullNode, Types.LongType.get()));
        assertNull(PartitionData.getValue(nullNode, Types.StringType.get()));
        assertNull(PartitionData.getValue(nullNode, Types.DecimalType.of(10, 2)));
    }

    @Test
    public void testGetValueWithDecimalFromLong()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(10, 2);
        JsonNode longNode = JSON.numberNode(12345L);

        BigDecimal result = (BigDecimal) PartitionData.getValue(longNode, decimalType);
        assertEquals(result, new BigDecimal("123.45"));
        assertEquals(result.scale(), 2);
        assertEquals(result.unscaledValue(), BigInteger.valueOf(12345L));
    }

    @Test
    public void testGetValueWithDecimalFromInt()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(5, 2);
        JsonNode intNode = JSON.numberNode(999);

        BigDecimal result = (BigDecimal) PartitionData.getValue(intNode, decimalType);
        assertEquals(result, new BigDecimal("9.99"));
        assertEquals(result.scale(), 2);
        assertEquals(result.unscaledValue(), BigInteger.valueOf(999));
    }

    @Test
    public void testGetValueWithDecimalFromBigInteger()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(20, 3);
        BigInteger bigInt = new BigInteger("123456789012345");
        JsonNode bigIntNode = JSON.numberNode(bigInt);

        BigDecimal result = (BigDecimal) PartitionData.getValue(bigIntNode, decimalType);
        assertEquals(result.scale(), 3);
        assertEquals(result.unscaledValue(), bigInt);
        assertEquals(result, new BigDecimal(bigInt, 3));
    }

    @Test
    public void testGetValueWithDecimalFromDecimal()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(10, 4);
        JsonNode decimalNode = JSON.numberNode(new BigDecimal("123.456"));

        BigDecimal result = (BigDecimal) PartitionData.getValue(decimalNode, decimalType);
        assertEquals(result, new BigDecimal("123.4560"));
        assertEquals(result.scale(), 4);
    }

    @Test
    public void testGetValueWithDecimalZeroScale()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(10, 0);
        JsonNode longNode = JSON.numberNode(12345L);

        BigDecimal result = (BigDecimal) PartitionData.getValue(longNode, decimalType);

        assertEquals(result, new BigDecimal("12345"));
        assertEquals(result.scale(), 0);
    }

    @Test
    public void testGetValueWithDecimalLargeScale()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(15, 10);
        JsonNode intNode = JSON.numberNode(123);

        BigDecimal result = (BigDecimal) PartitionData.getValue(intNode, decimalType);
        assertEquals(result.scale(), 10);
        assertEquals(result.unscaledValue(), BigInteger.valueOf(123));
    }

    @Test
    public void testGetValueWithDecimalNegativeValue()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(10, 2);
        JsonNode longNode = JSON.numberNode(-12345L);

        BigDecimal result = (BigDecimal) PartitionData.getValue(longNode, decimalType);

        assertEquals(result, new BigDecimal("-123.45"));
        assertEquals(result.scale(), 2);
    }

    @Test
    public void testGetValueWithDecimalVeryLargeNumber()
    {
        Types.DecimalType decimalType = Types.DecimalType.of(38, 5);
        BigInteger veryLarge = new BigInteger("12345678901234567890123456789012");
        JsonNode bigIntNode = JSON.numberNode(veryLarge);

        BigDecimal result = (BigDecimal) PartitionData.getValue(bigIntNode, decimalType);

        assertEquals(result.scale(), 5);
        assertEquals(result.unscaledValue(), veryLarge);
    }

    @Test
    public void testJsonRoundTripWithDecimals()
    {
        // This tests all new code paths: isLong(), isInt(), isBigInteger(), and fallback
        org.apache.iceberg.types.Type[] types = new org.apache.iceberg.types.Type[] {
                Types.DecimalType.of(15, 2),    // Will deserialize from long (12345L in JSON)
                Types.DecimalType.of(10, 3),    // Will deserialize from long (9876543210L in JSON)
                Types.DecimalType.of(5, 2),     // Will deserialize from int (999 in JSON)
                Types.DecimalType.of(20, 5),    // Will deserialize from decimal
                Types.DecimalType.of(38, 10)    // Will deserialize from BigInteger
        };

        Object[] values = new Object[] {
                12345L,
                9876543210L,
                999,
                new BigDecimal("123456.78901"),
                new BigDecimal("1234567890123456789012345678.0123456789")
        };

        PartitionData original = new PartitionData(values);
        String json = original.toJson();

        PartitionData deserialized = PartitionData.fromJson(json, types);
        assertEquals(deserialized.get(0, BigDecimal.class), new BigDecimal("123.45"));
        assertEquals(deserialized.get(0, BigDecimal.class).scale(), 2);
        assertEquals(deserialized.get(1, BigDecimal.class), new BigDecimal("9876543.210"));
        assertEquals(deserialized.get(1, BigDecimal.class).scale(), 3);
        assertEquals(deserialized.get(2, BigDecimal.class), new BigDecimal("9.99"));
        assertEquals(deserialized.get(2, BigDecimal.class).scale(), 2);
        assertEquals(deserialized.get(3, BigDecimal.class).compareTo(new BigDecimal("123456.78901")), 0);
        assertEquals(deserialized.get(3, BigDecimal.class).scale(), 5);
        assertEquals(deserialized.get(4, BigDecimal.class).compareTo(new BigDecimal("1234567890123456789012345678.0123456789")), 0);
        assertEquals(deserialized.get(4, BigDecimal.class).scale(), 10);
    }

    @Test
    public void testJsonRoundTripWithMixedTypes()
    {
        org.apache.iceberg.types.Type[] types = new org.apache.iceberg.types.Type[] {
                Types.IntegerType.get(),
                Types.LongType.get(),
                Types.DecimalType.of(10, 2),
                Types.StringType.get(),
                Types.DecimalType.of(5, 3)
        };

        Object[] values = new Object[] {
                42,
                9876543210L,
                new BigDecimal("999.99"),
                "test_partition",
                new BigDecimal("12.345")
        };

        PartitionData original = new PartitionData(values);
        String json = original.toJson();

        PartitionData deserialized = PartitionData.fromJson(json, types);

        assertEquals(deserialized.get(0, Integer.class), Integer.valueOf(42));
        assertEquals(deserialized.get(1, Long.class), Long.valueOf(9876543210L));
        assertEquals(deserialized.get(2, BigDecimal.class).compareTo(new BigDecimal("999.99")), 0);
        assertEquals(deserialized.get(3, String.class), "test_partition");
        assertEquals(deserialized.get(4, BigDecimal.class).compareTo(new BigDecimal("12.345")), 0);
    }
}
