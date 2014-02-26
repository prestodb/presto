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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.FunctionAssertions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestVarcharOperators
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("'foo'", "foo");
        assertFunction("'bar'", "bar");
        assertFunction("''", "");
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("'foo' || 'foo'", "foo" + "foo");
        assertFunction("'foo' || 'bar'", "foo" + "bar");
        assertFunction("'bar' || 'foo'", "bar" + "foo");
        assertFunction("'bar' || 'bar'", "bar" + "bar");
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("'foo' = 'foo'", true);
        assertFunction("'foo' = 'bar'", false);
        assertFunction("'bar' = 'foo'", false);
        assertFunction("'bar' = 'bar'", true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("'foo' <> 'foo'", false);
        assertFunction("'foo' <> 'bar'", true);
        assertFunction("'bar' <> 'foo'", true);
        assertFunction("'bar' <> 'bar'", false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("'foo' < 'foo'", false);
        assertFunction("'foo' < 'bar'", false);
        assertFunction("'bar' < 'foo'", true);
        assertFunction("'bar' < 'bar'", false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("'foo' <= 'foo'", true);
        assertFunction("'foo' <= 'bar'", false);
        assertFunction("'bar' <= 'foo'", true);
        assertFunction("'bar' <= 'bar'", true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("'foo' > 'foo'", false);
        assertFunction("'foo' > 'bar'", true);
        assertFunction("'bar' > 'foo'", false);
        assertFunction("'bar' > 'bar'", false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("'foo' >= 'foo'", true);
        assertFunction("'foo' >= 'bar'", true);
        assertFunction("'bar' >= 'foo'", false);
        assertFunction("'bar' >= 'bar'", true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("'foo' BETWEEN 'foo' AND 'foo'", true);
        assertFunction("'foo' BETWEEN 'foo' AND 'bar'", false);

        assertFunction("'foo' BETWEEN 'bar' AND 'foo'", true);
        assertFunction("'foo' BETWEEN 'bar' AND 'bar'", false);

        assertFunction("'bar' BETWEEN 'foo' AND 'foo'", false);
        assertFunction("'bar' BETWEEN 'foo' AND 'bar'", false);

        assertFunction("'bar' BETWEEN 'bar' AND 'foo'", true);
        assertFunction("'bar' BETWEEN 'bar' AND 'bar'", true);
    }
}
