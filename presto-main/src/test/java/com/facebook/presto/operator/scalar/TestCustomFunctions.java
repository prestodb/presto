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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCustomFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setupClass()
    {
        functionAssertions = new FunctionAssertions().addScalarFunctions(CustomAdd.class);
    }

    @Test
    public void testCustomAdd()
    {
        functionAssertions.assertFunction("custom_add(123, 456)", 579L);
    }
}
