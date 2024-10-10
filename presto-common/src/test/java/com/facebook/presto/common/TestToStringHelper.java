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
package com.facebook.presto.common;

import com.facebook.presto.common.Utils.ToStringHelper;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static com.facebook.presto.common.Utils.toStringHelper;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestToStringHelper
{
    @Test
    public void testConstructorInstance()
    {
        String toTest = toStringHelper(this).toString();
        assertEquals("TestToStringHelper{}", toTest);
    }

    @Test
    public void testConstructorLenientInstance()
    {
        String toTest = toStringHelper(this).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    @Test
    public void testConstructorInnerClass()
    {
        String toTest = toStringHelper(new TestClass()).toString();
        assertEquals("TestClass{}", toTest);
    }

    @Test
    public void testConstructorLenientInnerClass()
    {
        String toTest = toStringHelper(new TestClass()).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    @Test
    public void testConstructorAnonymousClass()
    {
        String toTest = toStringHelper(new Object() {}).toString();
        assertEquals("{}", toTest);
    }

    @Test
    public void testConstructorLenientAnonymousClass()
    {
        String toTest = toStringHelper(new Object() {}).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    @Test
    public void testConstructorClassObject()
    {
        String toTest = toStringHelper(TestClass.class).toString();
        assertEquals("TestClass{}", toTest);
    }

    @Test
    public void testConstructorLenientClassObject()
    {
        String toTest = toStringHelper(TestClass.class).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    @Test
    public void testConstructorStringObject()
    {
        String toTest = toStringHelper("FooBar").toString();
        assertEquals("FooBar{}", toTest);
    }

    @Test
    public void testToStringHelperLocalInnerClass()
    {
        // Local inner classes have names ending like "Outer.$1Inner"
        class LocalInnerClass {}
        String toTest = toStringHelper(new LocalInnerClass()).toString();
        assertEquals("LocalInnerClass{}", toTest);
    }

    @Test
    public void testToStringHelperLenientLocalInnerClass()
    {
        class LocalInnerClass {}
        String toTest = toStringHelper(new LocalInnerClass()).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    @Test
    public void testToStringHelperLocalInnerNestedClass()
    {
        class LocalInnerClass
        {
            class LocalInnerNestedClass {}
        }
        String toTest =
                toStringHelper(new LocalInnerClass().new LocalInnerNestedClass()).toString();
        assertEquals("LocalInnerNestedClass{}", toTest);
    }

    @Test
    public void testToStringHelperLenientLocalInnerNestedClass()
    {
        class LocalInnerClass
        {
            class LocalInnerNestedClass {}
        }
        String toTest =
                toStringHelper(new LocalInnerClass().new LocalInnerNestedClass()).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    @Test
    public void testToStringHelperMoreThanNineAnonymousClasses()
    {
        // The nth anonymous class has a name ending like "Outer.$n"
        Object unused1 = new Object() {};
        Object unused2 = new Object() {};
        Object unused3 = new Object() {};
        Object unused4 = new Object() {};
        Object unused5 = new Object() {};
        Object unused6 = new Object() {};
        Object unused7 = new Object() {};
        Object unused8 = new Object() {};
        Object unused9 = new Object() {};
        Object o10 = new Object() {};
        String toTest = toStringHelper(o10).toString();
        assertEquals("{}", toTest);
    }

    @Test
    public void testToStringHelperLenientMoreThanNineAnonymousClasses()
    {
        // The nth anonymous class has a name ending like "Outer.$n"
        Object unused1 = new Object() {};
        Object unused2 = new Object() {};
        Object unused3 = new Object() {};
        Object unused4 = new Object() {};
        Object unused5 = new Object() {};
        Object unused6 = new Object() {};
        Object unused7 = new Object() {};
        Object unused8 = new Object() {};
        Object unused9 = new Object() {};
        Object o10 = new Object() {};
        String toTest = toStringHelper(o10).toString();
        assertTrue(toTest.matches(".*\\{\\}"), toTest);
    }

    // all remaining test are on an inner class with various fields
    @Test
    public void testToStringOneField()
    {
        String toTest = toStringHelper(new TestClass()).add("field1", "Hello").toString();
        assertEquals("TestClass{field1=Hello}", toTest);
    }

    @Test
    public void testToStringOneIntegerField()
    {
        String toTest =
                toStringHelper(new TestClass()).add("field1", Integer.valueOf(42)).toString();
        assertEquals("TestClass{field1=42}", toTest);
    }

    @Test
    public void testToStringNullInteger()
    {
        String toTest =
                toStringHelper(new TestClass()).add("field1", (Integer) null).toString();
        assertEquals("TestClass{field1=null}", toTest);
    }

    @Test
    public void testToStringLenientOneField()
    {
        String toTest = toStringHelper(new TestClass()).add("field1", "Hello").toString();
        assertTrue(toTest.matches(".*\\{field1\\=Hello\\}"), toTest);
    }

    @Test
    public void testToStringLenientOneIntegerField()
    {
        String toTest =
                toStringHelper(new TestClass()).add("field1", Integer.valueOf(42)).toString();
        assertTrue(toTest.matches(".*\\{field1\\=42\\}"), toTest);
    }

    @Test
    public void testToStringLenientNullInteger()
    {
        String toTest =
                toStringHelper(new TestClass()).add("field1", (Integer) null).toString();
        assertTrue(toTest.matches(".*\\{field1\\=null\\}"), toTest);
    }

    @Test
    public void testToStringComplexFields()
    {
        Map<String, Integer> map =
                ImmutableMap.<String, Integer>builder().put("abc", 1).put("def", 2).put("ghi", 3).build();
        String toTest =
                toStringHelper(new TestClass())
                        .add("field1", "This is string.")
                        .add("field2", Arrays.asList("abc", "def", "ghi"))
                        .add("field3", map)
                        .toString();
        final String expected =
                "TestClass{"
                        + "field1=This is string., field2=[abc, def, ghi], field3={abc=1, def=2, ghi=3}}";

        assertEquals(expected, toTest);
    }

    @Test
    public void testToStringLenientComplexFields()
    {
        Map<String, Integer> map =
                ImmutableMap.<String, Integer>builder().put("abc", 1).put("def", 2).put("ghi", 3).build();
        String toTest =
                toStringHelper(new TestClass())
                        .add("field1", "This is string.")
                        .add("field2", Arrays.asList("abc", "def", "ghi"))
                        .add("field3", map)
                        .toString();
        final String expectedRegex =
                ".*\\{"
                        + "field1\\=This is string\\., "
                        + "field2\\=\\[abc, def, ghi\\], "
                        + "field3=\\{abc\\=1, def\\=2, ghi\\=3\\}\\}";

        assertTrue(toTest.matches(expectedRegex), toTest);
    }

    @Test
    public void testToStringAddWithNullName()
    {
        ToStringHelper helper = toStringHelper(new TestClass());
        assertThrows(NullPointerException.class, () -> helper.add(null, "Hello"));
    }

    @Test
    public void testToStringAddWithNullValue()
    {
        final String result = toStringHelper(new TestClass()).add("Hello", null).toString();

        assertEquals("TestClass{Hello=null}", result);
    }

    @Test
    public void testToStringLenientAddWithNullValue()
    {
        final String result = toStringHelper(new TestClass()).add("Hello", null).toString();
        assertTrue(result.matches(".*\\{Hello\\=null\\}"), result);
    }

    @Test
    public void testToStringOmitNullValuesOneField()
    {
        String toTest =
                toStringHelper(new TestClass()).omitNullValues().add("field1", null).toString();
        assertEquals("TestClass{}", toTest);
    }

    @Test
    public void testToStringOmitNullValuesManyFieldsFirstNull()
    {
        String toTest =
                toStringHelper(new TestClass())
                        .omitNullValues()
                        .add("field1", null)
                        .add("field2", "Googley")
                        .add("field3", "World")
                        .toString();
        assertEquals("TestClass{field2=Googley, field3=World}", toTest);
    }

    @Test
    public void testToStringOmitNullValuesManyFieldsOmitAfterNull()
    {
        String toTest =
                toStringHelper(new TestClass())
                        .add("field1", null)
                        .add("field2", "Googley")
                        .add("field3", "World")
                        .omitNullValues()
                        .toString();
        assertEquals("TestClass{field2=Googley, field3=World}", toTest);
    }

    @Test
    public void testToStringOmitNullValuesManyFieldsLastNull()
    {
        String toTest =
                toStringHelper(new TestClass())
                        .omitNullValues()
                        .add("field1", "Hello")
                        .add("field2", "Googley")
                        .add("field3", null)
                        .toString();
        assertEquals("TestClass{field1=Hello, field2=Googley}", toTest);
    }

    @Test
    public void testToStringOmitNullValuesDifferentOrder()
    {
        String expected = "TestClass{field1=Hello, field2=Googley, field3=World}";
        String toTest1 =
                toStringHelper(new TestClass())
                        .omitNullValues()
                        .add("field1", "Hello")
                        .add("field2", "Googley")
                        .add("field3", "World")
                        .toString();
        String toTest2 =
                toStringHelper(new TestClass())
                        .add("field1", "Hello")
                        .add("field2", "Googley")
                        .omitNullValues()
                        .add("field3", "World")
                        .toString();
        assertEquals(expected, toTest1);
        assertEquals(expected, toTest2);
    }

    @Test
    public void testToStringOmitNullValuesCanBeCalledManyTimes()
    {
        String toTest = toStringHelper(new TestClass())
                .omitNullValues()
                .omitNullValues()
                .add("field1", "Hello")
                .omitNullValues()
                .add("field2", "Googley")
                .omitNullValues()
                .add("field3", "World")
                .toString();
        assertEquals("TestClass{field1=Hello, field2=Googley, field3=World}", toTest);
    }

    @Test
    public void testToStringHelperWithArrays()
    {
        String[] strings = {"hello", "world"};
        int[] ints = {2, 42};
        Object[] objects = {"obj"};
        String[] arrayWithNull = {null};
        Object[] empty = {};
        String toTest =
                toStringHelper("TSH")
                        .add("strings", strings)
                        .add("ints", ints)
                        .add("objects", objects)
                        .add("arrayWithNull", arrayWithNull)
                        .add("empty", empty)
                        .toString();
        assertEquals(
                "TSH{strings=[hello, world], ints=[2, 42], objects=[obj], arrayWithNull=[null], empty=[]}",
                toTest);
    }

    /**
     * Test class for testing formatting of inner classes.
     */
    private static class TestClass {}
}
