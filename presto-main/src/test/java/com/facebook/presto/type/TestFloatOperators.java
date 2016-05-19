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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.FloatType.FLOAT;

public class TestFloatOperators
        extends AbstractTestFunctions
{
    @Test
    public void testTypeConstructor()
            throws Exception
    {
        assertFunction("FLOAT'12.2'", FLOAT, 12.2f);
        assertFunction("FLOAT'-17.76'", FLOAT, -17.76f);
        assertFunction("FLOAT'NaN'", FLOAT, Float.NaN);
        assertFunction("FLOAT'Infinity'", FLOAT, Float.POSITIVE_INFINITY);
        assertFunction("FLOAT'-Infinity'", FLOAT, Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-FLOAT'12.34'", FLOAT, -12.34f);
        assertFunction("-FLOAT'-17.34'", FLOAT, 17.34f);
        assertFunction("-FLOAT'-0.0'", FLOAT, -(-0.0f));
    }
}
