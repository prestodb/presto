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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.hive.HiveCoercer.IntegerNumberToVarcharCoercer;
import com.facebook.presto.hive.HiveCoercer.VarcharToIntegerNumberCoercer;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCoercingFilters
{
    @Test
    public void testIntegerToVarchar()
    {
        TupleDomainFilter filter = BytesRange.of("10".getBytes(), false, "10".getBytes(), false, false);

        HiveCoercer coercer = new IntegerNumberToVarcharCoercer(INTEGER, VARCHAR);

        TupleDomainFilter coercingFilter = coercer.toCoercingFilter(filter, new Subfield("c"));

        assertTrue(coercingFilter.testLong(10));
        assertFalse(coercingFilter.testLong(25));
        assertFalse(coercingFilter.testNull());
    }

    @Test
    public void testVarcharToInteger()
    {
        TupleDomainFilter filter = BigintRange.of(100, Integer.MAX_VALUE, false);

        HiveCoercer coercer = new VarcharToIntegerNumberCoercer(VARCHAR, INTEGER);

        TupleDomainFilter coercingFilter = coercer.toCoercingFilter(filter, new Subfield("c"));

        assertTrue(coercingFilter.testLength(1));
        assertTrue(coercingFilter.testLength(2));
        assertTrue(coercingFilter.testLength(3));

        assertTrue(coercingFilter.testBytes("100".getBytes(), 0, 3));
        assertTrue(coercingFilter.testBytes("145".getBytes(), 0, 3));
        assertTrue(coercingFilter.testBytes("2147483647".getBytes(), 0, 10));

        assertFalse(coercingFilter.testBytes("50".getBytes(), 0, 2));
        assertFalse(coercingFilter.testBytes("-50".getBytes(), 0, 3));

        // parsing error
        assertFalse(coercingFilter.testBytes("abc".getBytes(), 0, 3));

        // out of range
        assertFalse(coercingFilter.testBytes("2147483648".getBytes(), 0, 10));

        assertFalse(coercingFilter.testNull());
    }
}
