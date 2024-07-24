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

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;

public class TestFormatFunction
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @Test
    public void testFormat()
    {
        assertFormat("format('%.4f', pi())", "3.1416");
        assertFormat("format('%.5f', pi())", "3.14159");
        assertFormat("format('%,.2f', 1234567.89)", "1,234,567.89");
        assertFormat("format('%1$s %1$f %1$.2f', decimal '9.12345678')", "9.12345678 9.123457 9.12");

    }

    private void assertFormat(String projection, String expected)
    {
        assertFunction(projection, VARCHAR, expected);
    }
}