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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;

public class TestTryFunction
        extends AbstractTestFunctions
{
    @Test
    public void testTryFunction()
    {
        assertFunction("\"$internal$try_function\"(() -> 42)", INTEGER, 42);
        assertFunction("\"$internal$try_function\"(() -> 1/0)", INTEGER, null);
    }
}
