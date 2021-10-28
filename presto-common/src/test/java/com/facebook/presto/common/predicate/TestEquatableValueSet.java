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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockJsonSerde;
import com.facebook.presto.common.type.TestingIdType;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestEquatableValueSet
{
    @Test
    public void testEmptySet()
    {
        EquatableValueSet equatables = EquatableValueSet.none(TestingIdType.ID);
        assertEquals(equatables.getType(), TestingIdType.ID);
        assertTrue(equatables.isNone());
        assertFalse(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertTrue(equatables.isWhiteList());
        assertEquals(equatables.getValues().size(), 0);
        assertEquals(equatables.complement(), EquatableValueSet.all(TestingIdType.ID));
        assertFalse(equatables.containsValue(0L));
        assertFalse(equatables.containsValue(1L));
    }

    @Test
    public void testEntireSet()
    {
        EquatableValueSet equatables = EquatableValueSet.all(TestingIdType.ID);
        assertEquals(equatables.getType(), TestingIdType.ID);
        assertFalse(equatables.isNone());
        assertTrue(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertFalse(equatables.isWhiteList());
        assertEquals(equatables.getValues().size(), 0);
        assertEquals(equatables.complement(), EquatableValueSet.none(TestingIdType.ID));
        assertTrue(equatables.containsValue(0L));
        assertTrue(equatables.containsValue(1L));
    }

    @Test
    public void testSingleValue()
    {
        EquatableValueSet equatables = EquatableValueSet.of(TestingIdType.ID, 10L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(TestingIdType.ID).subtract(equatables);

        // Whitelist
        assertEquals(equatables.getType(), TestingIdType.ID);
        assertFalse(equatables.isNone());
        assertFalse(equatables.isAll());
        assertTrue(equatables.isSingleValue());
        assertTrue(equatables.isWhiteList());
        assertTrue(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(10L)));
        assertEquals(equatables.complement(), complement);
        assertFalse(equatables.containsValue(0L));
        assertFalse(equatables.containsValue(1L));
        assertTrue(equatables.containsValue(10L));

        // Blacklist
        assertEquals(complement.getType(), TestingIdType.ID);
        assertFalse(complement.isNone());
        assertFalse(complement.isAll());
        assertFalse(complement.isSingleValue());
        assertFalse(complement.isWhiteList());
        assertTrue(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(10L)));
        assertEquals(complement.complement(), equatables);
        assertTrue(complement.containsValue(0L));
        assertTrue(complement.containsValue(1L));
        assertFalse(complement.containsValue(10L));
    }

    @Test
    public void testMultipleValues()
    {
        EquatableValueSet equatables = EquatableValueSet.of(TestingIdType.ID, 1L, 2L, 3L, 1L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(TestingIdType.ID).subtract(equatables);

        // Whitelist
        assertEquals(equatables.getType(), TestingIdType.ID);
        assertFalse(equatables.isNone());
        assertFalse(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertTrue(equatables.isWhiteList());
        assertTrue(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(1L, 2L, 3L)));
        assertEquals(equatables.complement(), complement);
        assertFalse(equatables.containsValue(0L));
        assertTrue(equatables.containsValue(1L));
        assertTrue(equatables.containsValue(2L));
        assertTrue(equatables.containsValue(3L));
        assertFalse(equatables.containsValue(4L));

        // Blacklist
        assertEquals(complement.getType(), TestingIdType.ID);
        assertFalse(complement.isNone());
        assertFalse(complement.isAll());
        assertFalse(complement.isSingleValue());
        assertFalse(complement.isWhiteList());
        assertTrue(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(1L, 2L, 3L)));
        assertEquals(complement.complement(), equatables);
        assertTrue(complement.containsValue(0L));
        assertFalse(complement.containsValue(1L));
        assertFalse(complement.containsValue(2L));
        assertFalse(complement.containsValue(3L));
        assertTrue(complement.containsValue(4L));
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).getSingleValue(), 0L);
        assertThrows(IllegalStateException.class, () -> EquatableValueSet.all(TestingIdType.ID).getSingleValue());
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(EquatableValueSet.all(TestingIdType.ID).overlaps(EquatableValueSet.all(TestingIdType.ID)));
        assertFalse(EquatableValueSet.all(TestingIdType.ID).overlaps(EquatableValueSet.none(TestingIdType.ID)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));

        assertFalse(EquatableValueSet.none(TestingIdType.ID).overlaps(EquatableValueSet.all(TestingIdType.ID)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).overlaps(EquatableValueSet.none(TestingIdType.ID)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));

        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.all(TestingIdType.ID)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.none(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.of(TestingIdType.ID, 1L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L).complement()));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L).overlaps(EquatableValueSet.of(TestingIdType.ID, 1L).complement()));

        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).overlaps(EquatableValueSet.all(TestingIdType.ID)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).overlaps(EquatableValueSet.none(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).overlaps(EquatableValueSet.of(TestingIdType.ID, -1L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).overlaps(EquatableValueSet.of(TestingIdType.ID, -1L).complement()));

        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().overlaps(EquatableValueSet.all(TestingIdType.ID)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().overlaps(EquatableValueSet.none(TestingIdType.ID)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(TestingIdType.ID, -1L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(TestingIdType.ID, -1L).complement()));
    }

    @Test
    public void testContains()
    {
        assertTrue(EquatableValueSet.all(TestingIdType.ID).contains(EquatableValueSet.all(TestingIdType.ID)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).contains(EquatableValueSet.none(TestingIdType.ID)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).contains(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertTrue(EquatableValueSet.all(TestingIdType.ID).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));

        assertFalse(EquatableValueSet.none(TestingIdType.ID).contains(EquatableValueSet.all(TestingIdType.ID)));
        assertTrue(EquatableValueSet.none(TestingIdType.ID).contains(EquatableValueSet.none(TestingIdType.ID)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).contains(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertFalse(EquatableValueSet.none(TestingIdType.ID).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));

        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.all(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.none(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.of(TestingIdType.ID, 0L).complement()));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L).contains(EquatableValueSet.of(TestingIdType.ID, 1L).complement()));

        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.all(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.none(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 2L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.of(TestingIdType.ID, 0L).complement()));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).contains(EquatableValueSet.of(TestingIdType.ID, 1L).complement()));

        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().contains(EquatableValueSet.all(TestingIdType.ID)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().contains(EquatableValueSet.none(TestingIdType.ID)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().contains(EquatableValueSet.of(TestingIdType.ID, 0L)));
        assertTrue(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().contains(EquatableValueSet.of(TestingIdType.ID, -1L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().contains(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().contains(EquatableValueSet.of(TestingIdType.ID, -1L).complement()));
    }

    @Test
    public void testIntersect()
    {
        assertEquals(EquatableValueSet.none(TestingIdType.ID).intersect(EquatableValueSet.none(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.all(TestingIdType.ID).intersect(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.all(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).intersect(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).intersect(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.all(TestingIdType.ID).intersect(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).intersect(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).intersect(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().intersect(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().intersect(EquatableValueSet.of(TestingIdType.ID, 1L)), EquatableValueSet.of(TestingIdType.ID, 1L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).intersect(EquatableValueSet.of(TestingIdType.ID, 1L).complement()), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).intersect(EquatableValueSet.of(TestingIdType.ID, 0L, 2L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().intersect(EquatableValueSet.of(TestingIdType.ID, 0L, 2L)), EquatableValueSet.of(TestingIdType.ID, 2L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().intersect(EquatableValueSet.of(TestingIdType.ID, 0L, 2L).complement()), EquatableValueSet.of(TestingIdType.ID, 0L, 1L, 2L).complement());
    }

    @Test
    public void testUnion()
    {
        assertEquals(EquatableValueSet.none(TestingIdType.ID).union(EquatableValueSet.none(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.all(TestingIdType.ID).union(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.all(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).union(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.all(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).union(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.all(TestingIdType.ID).union(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.all(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).union(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).union(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L, 1L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().union(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.all(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().union(EquatableValueSet.of(TestingIdType.ID, 1L)), EquatableValueSet.of(TestingIdType.ID, 0L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).union(EquatableValueSet.of(TestingIdType.ID, 1L).complement()), EquatableValueSet.of(TestingIdType.ID, 1L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).union(EquatableValueSet.of(TestingIdType.ID, 0L, 2L)), EquatableValueSet.of(TestingIdType.ID, 0L, 1L, 2L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().union(EquatableValueSet.of(TestingIdType.ID, 0L, 2L)), EquatableValueSet.of(TestingIdType.ID, 1L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement().union(EquatableValueSet.of(TestingIdType.ID, 0L, 2L).complement()), EquatableValueSet.of(TestingIdType.ID, 0L).complement());
    }

    @Test
    public void testSubtract()
    {
        assertEquals(EquatableValueSet.all(TestingIdType.ID).subtract(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.all(TestingIdType.ID).subtract(EquatableValueSet.none(TestingIdType.ID)), EquatableValueSet.all(TestingIdType.ID));
        assertEquals(EquatableValueSet.all(TestingIdType.ID).subtract(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L).complement());
        assertEquals(EquatableValueSet.all(TestingIdType.ID).subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)), EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement());
        assertEquals(EquatableValueSet.all(TestingIdType.ID).subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()), EquatableValueSet.of(TestingIdType.ID, 0L, 1L));

        assertEquals(EquatableValueSet.none(TestingIdType.ID).subtract(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).subtract(EquatableValueSet.none(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).subtract(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.none(TestingIdType.ID).subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()), EquatableValueSet.none(TestingIdType.ID));

        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.none(TestingIdType.ID)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.of(TestingIdType.ID, 0L).complement()), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.of(TestingIdType.ID, 1L)), EquatableValueSet.of(TestingIdType.ID, 0L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.of(TestingIdType.ID, 1L).complement()), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()), EquatableValueSet.of(TestingIdType.ID, 0L));

        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.all(TestingIdType.ID)), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.none(TestingIdType.ID)), EquatableValueSet.of(TestingIdType.ID, 0L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.of(TestingIdType.ID, 0L)), EquatableValueSet.of(TestingIdType.ID, 0L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.of(TestingIdType.ID, 0L).complement()), EquatableValueSet.none(TestingIdType.ID));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.of(TestingIdType.ID, 1L)), EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.of(TestingIdType.ID, 1L).complement()), EquatableValueSet.of(TestingIdType.ID, 1L));
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L)), EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement());
        assertEquals(EquatableValueSet.of(TestingIdType.ID, 0L).complement().subtract(EquatableValueSet.of(TestingIdType.ID, 0L, 1L).complement()), EquatableValueSet.of(TestingIdType.ID, 1L));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableCollection()
    {
        EquatableValueSet.of(TestingIdType.ID, 1L).getValues().clear();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableValueEntries()
    {
        EquatableValueSet.of(TestingIdType.ID, 1L).getEntries().clear();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableIterator()
    {
        Iterator<Object> iterator = EquatableValueSet.of(TestingIdType.ID, 1L).getValues().iterator();
        iterator.next();
        iterator.remove();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableValueEntryIterator()
    {
        Iterator<EquatableValueSet.ValueEntry> iterator = EquatableValueSet.of(TestingIdType.ID, 1L).getEntries().iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        EquatableValueSet set = EquatableValueSet.all(TestingIdType.ID);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.none(TestingIdType.ID);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(TestingIdType.ID, 1L);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(TestingIdType.ID, 1L, 2L);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(TestingIdType.ID, 1L, 2L).complement();
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));
    }
}
