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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

public class TestDecimalParametricType
        extends AbstractTestFunctions
{
    @Test
    public void decimalIsCreatedWithPrecisionAndScaleDefined()
    {
        assertDecimalFunction("CAST(1 AS DECIMAL(2, 0))", decimal("01"));
        assertDecimalFunction("CAST(0.01 AS DECIMAL(2, 2))", decimal(".01"));
        assertDecimalFunction("CAST(0.02 AS DECIMAL(10, 10))", decimal(".0200000000"));
        assertDecimalFunction("CAST(0.02 AS DECIMAL(10, 8))", decimal("00.02000000"));
    }

    @Test
    public void decimalIsCreatedWithOnlyPrecisionDefined()
    {
        assertDecimalFunction("CAST(1 AS DECIMAL(2))", decimal("01"));
        assertDecimalFunction("CAST(-22 AS DECIMAL(3))", decimal("-022"));
        assertDecimalFunction("CAST(31.41 AS DECIMAL(4))", decimal("0031"));
    }

    @Test
    public void decimalIsCreatedWithoutParameters()
    {
        assertDecimalFunction("CAST(1 AS DECIMAL)", maxPrecisionDecimal(1));
        assertDecimalFunction("CAST(-22 AS DECIMAL)", maxPrecisionDecimal(-22));
        assertDecimalFunction("CAST(31.41 AS DECIMAL)", maxPrecisionDecimal(31));
    }

    @Test
    public void creatingDecimalRoundsValueProperly()
    {
        assertDecimalFunction("CAST(0.022 AS DECIMAL(4, 2))", decimal("00.02"));
        assertDecimalFunction("CAST(0.025 AS DECIMAL(4, 2))", decimal("00.03"));
        assertDecimalFunction("CAST(32.01 AS DECIMAL(3, 1))", decimal("32.0"));
        assertDecimalFunction("CAST(32.06 AS DECIMAL(3, 1))", decimal("32.1"));
        assertDecimalFunction("CAST(32.1 AS DECIMAL(3, 0))", decimal("032"));
        assertDecimalFunction("CAST(32.5 AS DECIMAL(3, 0))", decimal("033"));
        assertDecimalFunction("CAST(-0.022 AS DECIMAL(4, 2))", decimal("-00.02"));
        assertDecimalFunction("CAST(-0.025 AS DECIMAL(4, 2))", decimal("-00.03"));
    }

    @Test
    public void decimalIsNotCreatedWhenScaleExceedsPrecision()
    {
        assertInvalidFunction("CAST(1 AS DECIMAL(1,2))", "DECIMAL scale must be in range [0, precision]");
        assertInvalidFunction("CAST(-22 AS DECIMAL(20,21))", "DECIMAL scale must be in range [0, precision]");
        assertInvalidFunction("CAST(31.41 AS DECIMAL(0,1))", "DECIMAL precision must be in range [1, 38]");
    }

    @Test
    public void decimalWithZeroLengthCannotBeCreated()
    {
        assertInvalidFunction("CAST(1 AS DECIMAL(0,0))", "DECIMAL precision must be in range [1, 38]");
        assertInvalidFunction("CAST(0 AS DECIMAL(0,0))", "DECIMAL precision must be in range [1, 38]");
        assertInvalidFunction("CAST(1 AS DECIMAL(0))", "DECIMAL precision must be in range [1, 38]");
        assertInvalidFunction("CAST(0 AS DECIMAL(0))", "DECIMAL precision must be in range [1, 38]");
    }
}
