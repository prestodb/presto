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

public class BooleanOperatorsTest
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
        assertFunction("true", true);
        assertFunction("false", false);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("true = true", true);
        assertFunction("true = false", false);
        assertFunction("false = true", false);
        assertFunction("false = false", true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("true <> true", false);
        assertFunction("true <> false", true);
        assertFunction("false <> true", true);
        assertFunction("false <> false", false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("true < true", false);
        assertFunction("true < false", false);
        assertFunction("false < true", true);
        assertFunction("false < false", false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("true <= true", true);
        assertFunction("true <= false", false);
        assertFunction("false <= true", true);
        assertFunction("false <= false", true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("true > true", false);
        assertFunction("true > false", true);
        assertFunction("false > true", false);
        assertFunction("false > false", false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("true >= true", true);
        assertFunction("true >= false", true);
        assertFunction("false >= true", false);
        assertFunction("false >= false", true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("true BETWEEN true AND true", true);
        assertFunction("true BETWEEN true AND false", false);
        assertFunction("true BETWEEN false AND true", true);
        assertFunction("true BETWEEN false AND false", false);
        assertFunction("false BETWEEN true AND true", false);
        assertFunction("false BETWEEN true AND false", false);
        assertFunction("false BETWEEN false AND true", true);
        assertFunction("false BETWEEN false AND false", true);
    }

    @Test
    public void testCastToSlice()
            throws Exception
    {
        assertFunction("cast(true as varchar)", "true");
        assertFunction("cast(false as varchar)", "false");
    }

    @Test
    public void testCastFromSlice()
            throws Exception
    {
        assertFunction("cast('true' as boolean)", true);
        assertFunction("cast('false' as boolean)", false);
    }
}
