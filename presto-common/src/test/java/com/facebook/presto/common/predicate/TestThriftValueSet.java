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
package com.facebook.presto.common.predicate;

import org.testng.annotations.Test;

import java.lang.reflect.Proxy;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.JsonType.JSON;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

public class TestThriftValueSet
{
    @Test
    public void testUnionAlternatives()
    {
        EquatableValueSet equatable = EquatableValueSet.all(JSON);
        SortedRangeSet sorted = SortedRangeSet.all(BIGINT);
        AllOrNoneValueSet allOrNone = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        ValueSet.ThriftValueSet first = new ValueSet.ThriftValueSet(equatable);
        ValueSet.ThriftValueSet second = new ValueSet.ThriftValueSet(sorted);
        ValueSet.ThriftValueSet third = new ValueSet.ThriftValueSet(allOrNone);
        assertSame(first.getValueSet(), equatable);
        assertSame(second.getValueSet(), sorted);
        assertSame(third.getValueSet(), allOrNone);
        assertSame(first.getEquatableValueSet(), equatable);
        assertNull(first.getSortedRangeSet());
        assertNull(first.getAllOrNoneValueSet());
        assertNull(second.getEquatableValueSet());
        assertSame(second.getSortedRangeSet(), sorted);
        assertNull(second.getAllOrNoneValueSet());
        assertNull(third.getEquatableValueSet());
        assertNull(third.getSortedRangeSet());
        assertSame(third.getAllOrNoneValueSet(), allOrNone);
        assertEquals(first.getSetField(), (short) 1);
        assertEquals(second.getSetField(), (short) 2);
        assertEquals(third.getSetField(), (short) 3);
    }

    @Test
    public void testRejectsEmptyUnion()
    {
        assertThrows(NullPointerException.class, () -> new ValueSet.ThriftValueSet((ValueSet) null));
        assertThrows(NullPointerException.class, () -> new ValueSet.ThriftValueSet((EquatableValueSet) null));
        assertThrows(NullPointerException.class, () -> new ValueSet.ThriftValueSet((SortedRangeSet) null));
        assertThrows(NullPointerException.class, () -> new ValueSet.ThriftValueSet((AllOrNoneValueSet) null));
    }

    @Test
    public void testRejectsUnsupportedValueSet()
    {
        ValueSet unsupported = (ValueSet) Proxy.newProxyInstance(
                ValueSet.class.getClassLoader(), new Class<?>[] {ValueSet.class}, (proxy, method, args) -> null);
        assertThrows(IllegalArgumentException.class, () -> new ValueSet.ThriftValueSet(unsupported));
    }
}
