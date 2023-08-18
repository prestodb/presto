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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;

public class TestStringSqlFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testAscii()
    {
        assertFunction("ascii(123)", INTEGER, 49);
        assertFunction("ascii(3.14)", INTEGER, 51);
        assertFunction("ascii('a')", INTEGER, 97);
        assertFunction("ascii(cast('2023-01-01' as date))", INTEGER, 50);
        assertFunction("ascii(cast('2023-01-01 10:12:33' as timestamp))", INTEGER, 50);
        assertFunction("ascii(cast('1990-01-01 10:12:33' as timestamp))", INTEGER, 49);
        assertFunction("ascii(true)", INTEGER, 116);
        assertFunction("ascii(false)", INTEGER, 102);
        assertFunction("ascii('')", INTEGER, 0);
        assertFunction("ascii(null)", INTEGER, null);
        assertFunction("ascii(cast('Test' as varbinary))", INTEGER, 86);
    }
}
