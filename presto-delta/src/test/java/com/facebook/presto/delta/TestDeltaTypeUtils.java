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
package com.facebook.presto.delta;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.delta.DeltaErrorCode.DELTA_INVALID_PARTITION_VALUE;
import static com.facebook.presto.delta.DeltaTypeUtils.convertDeltaDataTypePrestoDataType;
import static com.facebook.presto.delta.DeltaTypeUtils.convertDeltaDataTypePrestoPhysicalType;
import static com.facebook.presto.delta.DeltaTypeUtils.convertPartitionValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDeltaTypeUtils
{
    private static final SchemaTableName TABLE = new SchemaTableName("test", "mapping");

    @Test
    public void partitionValueParsing()
    {
        assertPartitionValue("str", VARCHAR, Slices.utf8Slice("str"));
        assertPartitionValue("3", TINYINT, 3L);
        assertPartitionValue("344", SMALLINT, 344L);
        assertPartitionValue("323243", INTEGER, 323243L);
        assertPartitionValue("234234234233", BIGINT, 234234234233L);
        assertPartitionValue("3.234234", REAL, 1078918577L);
        assertPartitionValue("34534.23423423423", DOUBLE, 34534.23423423423);
        assertPartitionValue("2021-11-18", DATE, 18949L);
        assertPartitionValue("2021-11-18 05:23:43", TIMESTAMP, 1637213023000L);
        assertPartitionValue("true", BOOLEAN, true);
        assertPartitionValue("faLse", BOOLEAN, false);
        assertPartitionValue("234.5", createDecimalType(6, 3), 234500L);
        assertPartitionValue("12345678901234567890123.5", createDecimalType(25, 1), encodeUnscaledValue(new BigInteger("123456789012345678901235")));

        invalidPartitionValue("sdfsdf", BOOLEAN);
        invalidPartitionValue("sdfsdf", DATE);
        invalidPartitionValue("sdfsdf", TIMESTAMP);
        invalidPartitionValue("1234567890.5", createDecimalType(1, 1)); // invalid precision in value
    }

    @Test
    public void testPhysicalTypeIncludesNestedColumnMapping()
    {
        StructType leaf = new StructType(ImmutableList.of(
                mappedField("leaf", "col-leaf", StringType.STRING)));
        StructType value = new StructType(ImmutableList.of(
                mappedField("value", "col-value", LongType.LONG)));
        StructType type = new StructType(ImmutableList.of(
                mappedField("child", "col-child", IntegerType.INTEGER),
                mappedField("items", "col-items", new ArrayType(leaf, true)),
                mappedField("attributes", "col-attributes", new MapType(StringType.STRING, value, true))));

        TypeSignature logicalType = convertDeltaDataTypePrestoDataType(TABLE, "root", type);
        TypeSignature physicalType = convertDeltaDataTypePrestoPhysicalType(TABLE, "root", type);

        assertEquals(logicalType, parseTypeSignature("row(child integer,items array(row(leaf varchar)),attributes map(varchar,row(value bigint)))"));
        assertEquals(physicalType, parseTypeSignature(
                "row(\"col-child\" integer,\"col-items\" array(row(\"col-leaf\" varchar)),\"col-attributes\" map(varchar,row(\"col-value\" bigint)))"));
    }

    private static StructField mappedField(String logicalName, String physicalName, io.delta.kernel.types.DataType type)
    {
        FieldMetadata metadata = FieldMetadata.builder()
                .putLong("delta.columnMapping.id", 1)
                .putString("delta.columnMapping.physicalName", physicalName)
                .build();
        return new StructField(logicalName, type, true, metadata);
    }

    private void assertPartitionValue(String value, Type type, Object expected)
    {
        Object actual = convertPartitionValue("p1", value, type);
        assertEquals(actual, expected);
    }

    private void invalidPartitionValue(String value, Type type)
    {
        try {
            convertPartitionValue("p1", value, type);
            fail("expected to fail");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DELTA_INVALID_PARTITION_VALUE.toErrorCode());
            assertTrue(e.getMessage().matches("Can not parse partition value .* of type .* for partition column 'p1'"));
        }
    }
}
