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
package com.facebook.presto.common.type;

import com.facebook.presto.common.QualifiedObjectName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeUtils.containsDistinctType;
import static com.facebook.presto.common.type.TypeUtils.doubleCompare;
import static com.facebook.presto.common.type.TypeUtils.doubleEquals;
import static com.facebook.presto.common.type.TypeUtils.doubleHashCode;
import static com.facebook.presto.common.type.TypeUtils.realCompare;
import static com.facebook.presto.common.type.TypeUtils.realEquals;
import static com.facebook.presto.common.type.TypeUtils.realHashCode;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTypeUtils
{
    @Test
    public void testContainsDistinctType()
    {
        QualifiedObjectName distinctTypeName = QualifiedObjectName.valueOf("test.dt.int00");
        DistinctTypeInfo distinctTypeInfo = new DistinctTypeInfo(distinctTypeName, INTEGER.getTypeSignature(), Optional.empty(), false);
        DistinctType distinctType = new DistinctType(distinctTypeInfo, INTEGER, null);

        // check primitives
        assertFalse(containsDistinctType(ImmutableList.of(INTEGER, VARCHAR)));

        // check top level
        assertTrue(containsDistinctType(ImmutableList.of(distinctType)));

        // check first nesting level
        assertFalse(containsDistinctType(ImmutableList.of(RowType.anonymous(ImmutableList.of(INTEGER, VARCHAR)))));
        assertFalse(containsDistinctType(ImmutableList.of(new ArrayType(INTEGER))));
        assertTrue(containsDistinctType(ImmutableList.of(RowType.anonymous(ImmutableList.of(INTEGER, distinctType)))));
        assertTrue(containsDistinctType(ImmutableList.of(new ArrayType(distinctType))));

        // check deep nesting
        assertFalse(containsDistinctType(ImmutableList.of(new ArrayType(new ArrayType(INTEGER)))));
        assertFalse(containsDistinctType(ImmutableList.of(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, VARCHAR))))));
        assertTrue(containsDistinctType(ImmutableList.of(new ArrayType(new ArrayType(distinctType)))));
        assertTrue(containsDistinctType(ImmutableList.of(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, distinctType))))));
    }

    @Test
    public void testDoubleHashCode()
    {
        assertEquals(doubleHashCode(0), doubleHashCode(Double.parseDouble("-0")));
        //0x7ff8123412341234L is a different representation of NaN
        assertEquals(doubleHashCode(Double.NaN), doubleHashCode(longBitsToDouble(0x7ff8123412341234L)));
    }

    @Test
    public void testDoubleEquals()
    {
        assertTrue(doubleEquals(0, Double.parseDouble("-0")));
        //0x7ff8123412341234L is a different representation of NaN
        assertTrue(doubleEquals(Double.NaN, longBitsToDouble(0x7ff8123412341234L)));
    }

    @Test
    public void testDoubleCompare()
    {
        assertEquals(doubleCompare(0, Double.parseDouble("-0")), 0);
        assertEquals(doubleCompare(Double.NaN, Double.NaN), 0);
        //0x7ff8123412341234L is a different representation of NaN
        assertEquals(doubleCompare(Double.NaN, longBitsToDouble(0x7ff8123412341234L)), 0);
    }

    @Test
    public void testRealHashCode()
    {
        assertEquals(realHashCode(0), realHashCode(Float.parseFloat("-0")));
        // 0x7fc01234 is a different representation of NaN
        assertEquals(realHashCode(Float.NaN), realHashCode(intBitsToFloat(0x7fc01234)));
    }

    @Test
    public void testRealEquals()
    {
        assertTrue(realEquals(0, Float.parseFloat("-0")));
        assertTrue(realEquals(Float.NaN, Float.NaN));
        // 0x7fc01234 is a different representation of NaN
        assertTrue(realEquals(Float.NaN, intBitsToFloat(0x7fc01234)));
    }

    @Test
    public void testRealCompare()
    {
        assertEquals(realCompare(0, Float.parseFloat("-0")), 0);
        assertEquals(realCompare(Float.NaN, Float.NaN), 0);
        // 0x7fc01234 is a different representation of NaN
        assertEquals(realCompare(Float.NaN, intBitsToFloat(0x7fc01234)), 0);
    }
}
