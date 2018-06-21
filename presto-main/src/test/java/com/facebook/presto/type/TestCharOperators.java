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

import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class TestCharOperators
        extends AbstractTestFunctions
{
    @Test
    public void testEqual()
    {
        assertFunction("cast('foo' as char(3)) = cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) = cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) = cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) = cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(5)) = 'bar'", BOOLEAN, true);
        assertFunction("cast('bar' as char(5)) = 'bar   '", BOOLEAN, true);

        assertFunction("cast('a' as char(2)) = cast('a ' as char(2))", BOOLEAN, true);
        assertFunction("cast('a ' as char(2)) = cast('a' as char(2))", BOOLEAN, true);

        assertFunction("cast('a' as char(3)) = cast('a' as char(2))", BOOLEAN, true);
        assertFunction("cast('' as char(3)) = cast('' as char(2))", BOOLEAN, true);
        assertFunction("cast('' as char(2)) = cast('' as char(2))", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("cast('foo' as char(3)) <> cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) <> cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) <> cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) <> cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(5)) <> 'bar'", BOOLEAN, false);
        assertFunction("cast('bar' as char(5)) <> 'bar   '", BOOLEAN, false);

        assertFunction("cast('a' as char(2)) <> cast('a ' as char(2))", BOOLEAN, false);
        assertFunction("cast('a ' as char(2)) <> cast('a' as char(2))", BOOLEAN, false);

        assertFunction("cast('a' as char(3)) <> cast('a' as char(2))", BOOLEAN, false);
        assertFunction("cast('' as char(3)) <> cast('' as char(2))", BOOLEAN, false);
        assertFunction("cast('' as char(2)) <> cast('' as char(2))", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("cast('bar' as char(5)) < cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) < cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) < cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) < cast('bar' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) < cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) < cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) < cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) < cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) < cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foobar' as char(6)) < cast('foobaz' as char(6))", BOOLEAN, true);
        assertFunction("cast('foob r' as char(6)) < cast('foobar' as char(6))", BOOLEAN, true);
        assertFunction("cast('\0' as char(1)) < cast(' ' as char(1))", BOOLEAN, true);
        assertFunction("cast('\0' as char(1)) < cast('' as char(0))", BOOLEAN, true);
        assertFunction("cast('abc\0' as char(4)) < cast('abc' as char(4))", BOOLEAN, true); // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertFunction("cast('\0' as char(1)) < cast('\0 ' as char(2))", BOOLEAN, false);
        assertFunction("cast('\0' as char(2)) < cast('\0 ' as char(2))", BOOLEAN, false); // '\0' is implicitly padded with spaces -> both are equal
        assertFunction("cast('\0 a' as char(3)) < cast('\0' as char(3))", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("cast('bar' as char(5)) <= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) <= cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) <= cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) <= cast('bar' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) <= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) <= cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) <= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) <= cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) <= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foobar' as char(6)) <= cast('foobaz' as char(6))", BOOLEAN, true);
        assertFunction("cast('foob r' as char(6)) <= cast('foobar' as char(6))", BOOLEAN, true);
        assertFunction("cast('\0' as char(1)) <= cast(' ' as char(1))", BOOLEAN, true);
        assertFunction("cast('\0' as char(1)) <= cast('' as char(0))", BOOLEAN, true);
        assertFunction("cast('abc\0' as char(4)) <= cast('abc' as char(4))", BOOLEAN, true); // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertFunction("cast('\0' as char(1)) <= cast('\0 ' as char(2))", BOOLEAN, true); // length mismatch, coercion to VARCHAR applies
        assertFunction("cast('\0' as char(2)) <= cast('\0 ' as char(2))", BOOLEAN, true); // '\0' is implicitly padded with spaces -> both are equal
        assertFunction("cast('\0 a' as char(3)) <= cast('\0' as char(3))", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("cast('bar' as char(5)) > cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) > cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) > cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) > cast('bar' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) > cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) > cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) > cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) > cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) > cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foobar' as char(6)) > cast('foobaz' as char(6))", BOOLEAN, false);
        assertFunction("cast('foob r' as char(6)) > cast('foobar' as char(6))", BOOLEAN, false);
        assertFunction("cast(' ' as char(1)) > cast('\0' as char(1))", BOOLEAN, true);
        assertFunction("cast('' as char(0)) > cast('\0' as char(1))", BOOLEAN, true);
        assertFunction("cast('abc' as char(4)) > cast('abc\0' as char(4))", BOOLEAN, true); // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertFunction("cast('\0 ' as char(2)) > cast('\0' as char(1))", BOOLEAN, false);
        assertFunction("cast('\0 ' as char(2)) > cast('\0' as char(2))", BOOLEAN, false); // '\0' is implicitly padded with spaces -> both are equal
        assertFunction("cast('\0 a' as char(3)) > cast('\0' as char(3))", BOOLEAN, true);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("cast('bar' as char(5)) >= cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(5)) >= cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) >= cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) >= cast('bar' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) >= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) >= cast('foo' as char(5))", BOOLEAN, true);
        assertFunction("cast('foo' as char(5)) >= cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) >= cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) >= cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foobar' as char(6)) >= cast('foobaz' as char(6))", BOOLEAN, false);
        assertFunction("cast('foob r' as char(6)) >= cast('foobar' as char(6))", BOOLEAN, false);
        assertFunction("cast(' ' as char(1)) >= cast('\0' as char(1))", BOOLEAN, true);
        assertFunction("cast('' as char(0)) >= cast('\0' as char(1))", BOOLEAN, true);
        assertFunction("cast('abc' as char(4)) >= cast('abc\0' as char(4))", BOOLEAN, true); // 'abc' is implicitly padded with spaces -> 'abc' is greater
        assertFunction("cast('\0 ' as char(2)) >= cast('\0' as char(1))", BOOLEAN, true); // length mismatch, coercion to VARCHAR applies
        assertFunction("cast('\0 ' as char(2)) >= cast('\0' as char(2))", BOOLEAN, true); // '\0' is implicitly padded with spaces -> both are equal
        assertFunction("cast('\0 a' as char(3)) >= cast('\0' as char(3))", BOOLEAN, true);
    }

    @Test
    public void testBetween()
    {
        assertFunction("cast('bbb' as char(3)) BETWEEN cast('aaa' as char(3)) AND cast('ccc' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('bar' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) BETWEEN cast('zzz' as char(3)) AND cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) NOT BETWEEN cast('zzz' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);

        assertFunction("cast('foo' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('bar' as char(3))", BOOLEAN, false);

        assertFunction("cast('bar' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('bar' as char(3)) BETWEEN cast('foo' as char(3)) AND cast('bar' as char(3))", BOOLEAN, false);

        assertFunction("cast('bar' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) BETWEEN cast('bar' as char(3)) AND cast('bar' as char(3))", BOOLEAN, true);

        assertFunction("cast('\0 a' as char(3)) BETWEEN cast('\0' as char(3)) AND cast('\0a' as char(3))", BOOLEAN, true);

        // length based comparison
        assertFunction("cast('bar' as char(4)) BETWEEN cast('bar' as char(3)) AND cast('bar' as char(5))", BOOLEAN, true);
        assertFunction("cast('bar' as char(4)) BETWEEN cast('bar' as char(5)) AND cast('bar' as char(7))", BOOLEAN, true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("cast(NULL as char(3)) IS DISTINCT FROM cast(NULL as char(3))", BOOLEAN, false);
        assertFunction("cast(NULL as char(3)) IS DISTINCT FROM cast(NULL as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) IS DISTINCT FROM cast('foo' as char(5))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) IS DISTINCT FROM cast('foo' as char(3))", BOOLEAN, false);
        assertFunction("cast('foo' as char(3)) IS DISTINCT FROM cast('bar' as char(3))", BOOLEAN, true);
        assertFunction("cast('bar' as char(3)) IS DISTINCT FROM cast('foo' as char(3))", BOOLEAN, true);
        assertFunction("cast('foo' as char(3)) IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("cast('bar' as char(5)) IS DISTINCT FROM 'bar'", BOOLEAN, false);
        assertFunction("cast('bar' as char(5)) IS DISTINCT FROM 'bar   '", BOOLEAN, false);
        assertFunction("NULL IS DISTINCT FROM cast('foo' as char(3))", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "CAST(null AS CHAR(3))", BOOLEAN, true);
        assertOperator(INDETERMINATE, "CHAR '123'", BOOLEAN, false);
    }
}
