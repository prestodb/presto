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
package com.facebook.presto.hive.functions.type;

import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.TestRowType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.inject.Key;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static com.facebook.presto.hive.functions.type.ObjectEncoders.createEncoder;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestObjectEncoders
{
    private TestingPrestoServer server;
    private TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = createTestingPrestoServer();
        this.typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    @Test
    public void testPrimitiveObjectEncoders()
    {
        ObjectInspector inspector;
        ObjectEncoder encoder;

        inspector = writableLongObjectInspector;
        encoder = createEncoder(BIGINT, inspector);
        assertTrue(encoder.encode(new LongWritable(123456L)) instanceof Long);

        inspector = writableIntObjectInspector;
        encoder = createEncoder(INTEGER, inspector);
        assertTrue(encoder.encode(new IntWritable(12345)) instanceof Long);

        inspector = writableShortObjectInspector;
        encoder = createEncoder(SMALLINT, inspector);
        assertTrue(encoder.encode(new ShortWritable((short) 1234)) instanceof Long);

        inspector = writableByteObjectInspector;
        encoder = createEncoder(TINYINT, inspector);
        assertTrue(encoder.encode(new ByteWritable((byte) 123)) instanceof Long);

        inspector = writableBooleanObjectInspector;
        encoder = createEncoder(BOOLEAN, inspector);
        assertTrue(encoder.encode(new BooleanWritable(true)) instanceof Boolean);

        inspector = writableDoubleObjectInspector;
        encoder = createEncoder(DOUBLE, inspector);
        assertTrue(encoder.encode(new DoubleWritable(0.1)) instanceof Double);

        inspector = writableDateObjectInspector;
        encoder = createEncoder(DATE, inspector);
        assertTrue(encoder.encode(new DateWritable(DateTimeUtils.createDate(18380L))) instanceof Long);

        inspector = writableHiveDecimalObjectInspector;
        encoder = createEncoder(createDecimalType(11, 10), inspector);
        assertTrue(encoder.encode(new HiveDecimalWritable("1.2345678910")) instanceof Long);

        encoder = createEncoder(createDecimalType(34, 33), inspector);
        assertTrue(encoder.encode(new HiveDecimalWritable("1.281734081274028174012432412423134")) instanceof Slice);
    }

    @Test
    public void testTextObjectEncoders()
    {
        ObjectInspector inspector;
        ObjectEncoder encoder;

        inspector = writableBinaryObjectInspector;
        encoder = createEncoder(VARBINARY, inspector);
        assertTrue(encoder.encode(new BytesWritable(new byte[] {12, 34, 56})) instanceof Slice);

        inspector = writableStringObjectInspector;
        encoder = createEncoder(VARCHAR, inspector);
        assertTrue(encoder.encode(new Text("test_varchar")) instanceof Slice);

        inspector = writableStringObjectInspector;
        encoder = createEncoder(createCharType(10), inspector);
        assertTrue(encoder.encode(new Text("test_char")) instanceof Slice);
    }

    @Test
    public void testComplexObjectEncoders()
    {
        ObjectInspector inspector;
        ObjectEncoder encoder;

        inspector = ObjectInspectors.create(new ArrayType(BIGINT), typeManager);
        encoder = createEncoder(new ArrayType(BIGINT), inspector);
        assertTrue(encoder instanceof ObjectEncoders.ListObjectEncoder);
        Object arrayObject = encoder.encode(new Long[]{1L, 2L, 3L});
        assertTrue(arrayObject instanceof LongArrayBlock);
        assertEquals(((LongArrayBlock) arrayObject).getLong(0), 1L);
        assertEquals(((LongArrayBlock) arrayObject).getLong(1), 2L);
        assertEquals(((LongArrayBlock) arrayObject).getLong(2), 3L);

        inspector = ObjectInspectors.create(new MapType(
                VARCHAR,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation")), typeManager);
        encoder = createEncoder(new MapType(
                VARCHAR,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation")), inspector);
        assertTrue(encoder instanceof ObjectEncoders.MapObjectEncoder);
        assertTrue(encoder.encode(new HashMap<String, Long>(){}) instanceof SingleMapBlock);
    }
}
