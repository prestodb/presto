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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.Map.Entry;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveBucketing
{
    @Test
    public void testHashingBooleanLong()
            throws Exception
    {
        List<Entry<ObjectInspector, Object>> bindings = ImmutableList.<Entry<ObjectInspector, Object>>builder()
                .add(entry(javaBooleanObjectInspector, true))
                .add(entry(javaLongObjectInspector, 123L))
                .build();

        Optional<HiveBucket> bucket = HiveBucketing.getHiveBucket(bindings, 32);
        assertTrue(bucket.isPresent());
        assertEquals(bucket.get().getBucketCount(), 32);
        assertEquals(bucket.get().getBucketNumber(), 26);
    }

    @Test
    public void testHashingString()
            throws Exception
    {
        List<Entry<ObjectInspector, Object>> bindings = ImmutableList.<Entry<ObjectInspector, Object>>builder()
                .add(entry(javaStringObjectInspector, "sequencefile test"))
                .build();

        Optional<HiveBucket> bucket = HiveBucketing.getHiveBucket(bindings, 32);
        assertTrue(bucket.isPresent());
        assertEquals(bucket.get().getBucketCount(), 32);
        assertEquals(bucket.get().getBucketNumber(), 21);
    }

    private static Entry<ObjectInspector, Object> entry(ObjectInspector inspector, Object value)
    {
        return immutableEntry(inspector, value);
    }
}
