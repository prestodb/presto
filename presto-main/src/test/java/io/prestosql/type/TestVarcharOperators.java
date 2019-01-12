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

import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;

public class TestVarcharOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
    {
        assertFunction("'foo'", createVarcharType(3), "foo");
        assertFunction("'bar'", createVarcharType(3), "bar");
        assertFunction("''", createVarcharType(0), "");
    }

    @Test
    public void testTypeConstructor()
    {
        assertFunction("VARCHAR 'foo'", VARCHAR, "foo");
        assertFunction("VARCHAR 'bar'", VARCHAR, "bar");
        assertFunction("VARCHAR ''", VARCHAR, "");
    }

    @Test
    public void testAdd()
    {
        // TODO change expected return type to createVarcharType(6) when function resolving is fixed
        assertFunction("'foo' || 'foo'", VARCHAR, "foo" + "foo");
        assertFunction("'foo' || 'bar'", VARCHAR, "foo" + "bar");
        assertFunction("'bar' || 'foo'", VARCHAR, "bar" + "foo");
        assertFunction("'bar' || 'bar'", VARCHAR, "bar" + "bar");
        assertFunction("'bar' || 'barbaz'", VARCHAR, "bar" + "barbaz");
    }

    @Test
    public void testEqual()
    {
        assertFunction("'foo' = 'foo'", BOOLEAN, true);
        assertFunction("'foo' = 'bar'", BOOLEAN, false);
        assertFunction("'bar' = 'foo'", BOOLEAN, false);
        assertFunction("'bar' = 'bar'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("'foo' <> 'foo'", BOOLEAN, false);
        assertFunction("'foo' <> 'bar'", BOOLEAN, true);
        assertFunction("'bar' <> 'foo'", BOOLEAN, true);
        assertFunction("'bar' <> 'bar'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("'foo' < 'foo'", BOOLEAN, false);
        assertFunction("'foo' < 'bar'", BOOLEAN, false);
        assertFunction("'bar' < 'foo'", BOOLEAN, true);
        assertFunction("'bar' < 'bar'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("'foo' <= 'foo'", BOOLEAN, true);
        assertFunction("'foo' <= 'bar'", BOOLEAN, false);
        assertFunction("'bar' <= 'foo'", BOOLEAN, true);
        assertFunction("'bar' <= 'bar'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("'foo' > 'foo'", BOOLEAN, false);
        assertFunction("'foo' > 'bar'", BOOLEAN, true);
        assertFunction("'bar' > 'foo'", BOOLEAN, false);
        assertFunction("'bar' > 'bar'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("'foo' >= 'foo'", BOOLEAN, true);
        assertFunction("'foo' >= 'bar'", BOOLEAN, true);
        assertFunction("'bar' >= 'foo'", BOOLEAN, false);
        assertFunction("'bar' >= 'bar'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
    {
        assertFunction("'foo' BETWEEN 'foo' AND 'foo'", BOOLEAN, true);
        assertFunction("'foo' BETWEEN 'foo' AND 'bar'", BOOLEAN, false);

        assertFunction("'foo' BETWEEN 'bar' AND 'foo'", BOOLEAN, true);
        assertFunction("'foo' BETWEEN 'bar' AND 'bar'", BOOLEAN, false);

        assertFunction("'bar' BETWEEN 'foo' AND 'foo'", BOOLEAN, false);
        assertFunction("'bar' BETWEEN 'foo' AND 'bar'", BOOLEAN, false);

        assertFunction("'bar' BETWEEN 'bar' AND 'foo'", BOOLEAN, true);
        assertFunction("'bar' BETWEEN 'bar' AND 'bar'", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS VARCHAR) IS DISTINCT FROM CAST(NULL AS VARCHAR)", BOOLEAN, false);
        assertFunction("'foo' IS DISTINCT FROM 'foo'", BOOLEAN, false);
        assertFunction("'foo' IS DISTINCT FROM 'fo0'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM 'foo'", BOOLEAN, true);
        assertFunction("'foo' IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as varchar)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "'foo'", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(123456 as varchar)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(12345.0123 as varchar)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(true as varchar)", BOOLEAN, false);
    }
}
