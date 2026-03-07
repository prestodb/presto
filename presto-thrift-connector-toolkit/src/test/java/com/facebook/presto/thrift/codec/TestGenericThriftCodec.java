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
package com.facebook.presto.thrift.codec;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.drift.codec.ThriftCodecManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestGenericThriftCodec
{
    private ThriftCodecManager codecManager;

    @BeforeMethod
    public void setUp()
    {
        codecManager = new ThriftCodecManager();
    }

    @Test
    public void testSerializeAndDeserialize()
    {
        GenericThriftCodec<TestThriftObject> codec = new GenericThriftCodec<>(codecManager, TestThriftObject.class);

        TestThriftObject original = new TestThriftObject(42, "test-value");

        byte[] serialized = codec.serialize(original);

        assertNotNull(serialized);
        TestThriftObject deserialized = codec.deserialize(serialized);

        assertEquals(deserialized.getId(), original.getId());
        assertEquals(deserialized.getName(), original.getName());
    }

    @Test
    public void testSerializeNull()
    {
        GenericThriftCodec<TestThriftObject> codec = new GenericThriftCodec<>(codecManager, TestThriftObject.class);

        TestThriftObject original = new TestThriftObject(0, null);
        byte[] serialized = codec.serialize(original);

        assertNotNull(serialized);
        TestThriftObject deserialized = codec.deserialize(serialized);

        assertEquals(deserialized.getId(), original.getId());
        assertEquals(deserialized.getName(), original.getName());
    }

    @ThriftStruct
    public static class TestThriftObject
    {
        private final int id;
        private final String name;

        @ThriftConstructor
        public TestThriftObject(int id, String name)
        {
            this.id = id;
            this.name = name;
        }

        @ThriftField(1)
        public int getId()
        {
            return id;
        }

        @ThriftField(2)
        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestThriftObject that = (TestThriftObject) o;
            return id == that.id && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, name);
        }
    }
}
