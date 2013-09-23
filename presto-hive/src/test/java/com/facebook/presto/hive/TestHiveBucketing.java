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
package com.facebook.presto.hive;

import org.apache.hadoop.hive.ql.io.DefaultHivePartitioner;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;

public class TestHiveBucketing
{
    @Test
    public void testHashingDouble()
            throws Exception
    {
        DefaultHivePartitioner<HiveKey, Object> partitioner = new DefaultHivePartitioner<>();

        GenericUDFHash udf = new GenericUDFHash();
        ObjectInspector udfInspector = udf.initialize(new ObjectInspector[] {
                javaDoubleObjectInspector,
                javaFloatObjectInspector,
        });
        assertInstanceOf(udfInspector, IntObjectInspector.class);
        IntObjectInspector inspector = (IntObjectInspector) udfInspector;

        Object result = udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject(492.2d),
                new DeferredJavaObject((float)491.1000061035156d),
        });
        int hash = inspector.get(result);

        HiveKey hiveKey = new HiveKey();
        hiveKey.setHashCode(hash);
        int bucket = partitioner.getBucket(hiveKey, null, 32);

        assertEquals(bucket, 13);
    }

    @Test
    public void testHashingString()
            throws Exception
    {
        DefaultHivePartitioner<HiveKey, Object> partitioner = new DefaultHivePartitioner<>();

        GenericUDFHash udf = new GenericUDFHash();
        ObjectInspector udfInspector = udf.initialize(new ObjectInspector[] {
                javaStringObjectInspector,
        });
        assertInstanceOf(udfInspector, IntObjectInspector.class);
        IntObjectInspector inspector = (IntObjectInspector) udfInspector;

        Object result = udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject("sequencefile test"),
        });
        int hash = inspector.get(result);

        HiveKey hiveKey = new HiveKey();
        hiveKey.setHashCode(hash);
        int bucket = partitioner.getBucket(hiveKey, null, 32);

        assertEquals(bucket, 21);
    }
}
