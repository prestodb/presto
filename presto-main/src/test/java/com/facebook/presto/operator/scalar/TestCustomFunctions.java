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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestCustomFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setupClass()
    {
        functionAssertions = new FunctionAssertions().addScalarFunctions(CustomFunctions.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(functionAssertions);
        functionAssertions = null;
    }

    @Test
    public void testCustomAdd()
    {
        functionAssertions.assertFunction("custom_add(123, 456)", BIGINT, 579L);
    }

    @Test
    public void testSliceIsNull()
    {
        functionAssertions.assertFunction("custom_is_null(CAST(NULL AS VARCHAR))", BOOLEAN, true);
        functionAssertions.assertFunction("custom_is_null('not null')", BOOLEAN, false);
    }

    @Test
    public void testLongIsNull()
    {
        functionAssertions.assertFunction("custom_is_null(CAST(NULL AS BIGINT))", BOOLEAN, true);
        functionAssertions.assertFunction("custom_is_null(0)", BOOLEAN, false);
    }
}
